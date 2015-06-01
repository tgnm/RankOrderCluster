namespace RankOrderCluster

open System
open System.Diagnostics
open FSharp.Collections.ParallelSeq
open System.Collections.Generic
open System.Linq
open System.Collections.Concurrent
open Models


module RankOrderClusters =
    
    type MergeResult =
                | Merged of Cluster
                | Duplicate
                | NotMerged

    let ComputeClusters (faces:Dictionary<string,double[]>) (threshold:float) =

        // convert face features into a data point representation
        let allDataPoints =
            faces
            |> Seq.map (fun face -> { Id = Guid.NewGuid(); FilePath = face.Key; Features = face.Value; OrderList = [||] })
            |> Array.ofSeq

        // initialize with one cluser per face
        let allClusters =
            allDataPoints
            |> Array.map (fun face -> { Id = Guid.NewGuid(); Elements = [| face |]; OrderList = [||]; IndexedOrderList = new ConcurrentDictionary<Guid,int>() } )
        
        let setOrderLists (clusters:Cluster array) =
            let sw = System.Diagnostics.Stopwatch.StartNew()

            [| for i in 0 .. clusters.Length - 2 -> i |]
                |> PSeq.map (fun index ->
                    [| for x in index+1 .. clusters.Length - 1 -> x |]
                        |> Array.map (fun otherIndex -> 

                            let clusterA = clusters.[index]
                            let clusterB = clusters.[otherIndex]

                            if clusterA.OrderList.Length <> clusters.Length then
                                clusterA.OrderList <- Clusters.getClusterOrderList clusterA clusters
        
                            if clusterB.OrderList.Length <> clusters.Length then
                                clusterB.OrderList <- Clusters.getClusterOrderList clusterB clusters)) |> Seq.toArray |> ignore

            sw.Stop() |> ignore
            Debug.WriteLine("Sorted neighbors computed in {0} ms", sw.ElapsedMilliseconds)

        // step 3: finds mergeable pairs
        let getMergeablePairs (clusters:Cluster array) =
            
            setOrderLists clusters |> ignore

            let totalClusters = clusters.Length

            Debug.WriteLine(sprintf "Looking for mergeable clusters in %i clusters..." totalClusters)

            let filterCandidates (clusterA, clusterB) =
                
                let rankOrderDistance = Clusters.getClusterLevelRankOrderDistance clusterA clusterB clusters

                let normalizedDistance = Clusters.getClusterLevelNormalizedDistance clusterA clusterB allDataPoints 9

                match rankOrderDistance, normalizedDistance with
                | rankOrderDistance, normalizedDistance when rankOrderDistance < threshold && normalizedDistance < 1.0 -> Some (clusterA, clusterB)
                | _ -> None

            [| for i in 0 .. clusters.Length - 1 -> i |]
            |> Seq.map (fun index ->
                
                let clusterA = clusters.[index]

                let top = if clusterA.OrderList.Count() >= 20 then 20 else clusterA.OrderList.Count()

                let top20Neighbors = 
                    clusterA.OrderList 
                    |> Seq.take top
                    |> Array.ofSeq
                    |> Array.map (fun clusterB -> clusterA, clusterB)

                top20Neighbors
                |> PSeq.choose filterCandidates)
            |> PSeq.reduce (fun x acc -> acc.Concat x)
            |> Seq.toList
        

        let rec mergeClusters (clusters:Cluster array) =
        
            let watch = System.Diagnostics.Stopwatch.StartNew()

            let mergeable = getMergeablePairs clusters

            let flattenedMergeablesSet = 
                mergeable 
                |> Seq.collect (fun t -> [(fst t);(snd t)])
                |> Seq.distinctBy (fun c -> c.Id)
                |> Array.ofSeq

            // todo: use partition instead
            let allClustersWithoutMergeables =
                clusters
                |> Array.filter (fun x -> not (flattenedMergeablesSet.Any(fun xy -> xy.Id = x.Id)))
            
            Debug.WriteLine allClustersWithoutMergeables.Length

            let mergedClusters = new Dictionary<Guid,Cluster>()

            let clusterExistsInIndex clusterPair =
                (mergedClusters.ContainsKey((fst clusterPair).Id) || mergedClusters.ContainsKey((snd clusterPair).Id))

            let addClusterPairToIndex clusterPair =
                if (not (mergedClusters.ContainsKey ((fst clusterPair).Id))) then
                    mergedClusters.Add ((fst clusterPair).Id, (fst clusterPair))
                if (not (mergedClusters.ContainsKey ((snd clusterPair).Id))) then
                    mergedClusters.Add ((snd clusterPair).Id, (snd clusterPair))

            // step 9: transitive merge
            let mergeCandidatePairs (candidatePairs:(Cluster*Cluster) list) =
    
                let mergePair (clusterPair:Cluster*Cluster) =
                        let mergedElements =
                            [| fst clusterPair; snd clusterPair |]
                            |> Array.collect (fun c -> c.Elements)
                        { Id = Guid.NewGuid(); Elements = mergedElements; OrderList = [||]; IndexedOrderList = new ConcurrentDictionary<Guid,int>() }

                let tryMergeCompatiblePairs index =
                    [| for x in index + 1 .. candidatePairs.Length - 1 -> x |]
                    |> Array.map (fun otherIndex -> 
                        let clusterPairA = candidatePairs.[index] 
                        let clusterPairB = candidatePairs.[otherIndex]
                    
                        match clusterPairA, clusterPairB with
                            | pairA, pairB when clusterExistsInIndex pairA || clusterExistsInIndex pairB -> Duplicate
                            | (a,b), (c,d) when b.Id = c.Id -> 
                                let clustersinA = [ fst clusterPairA; snd clusterPairA ]
                                let clustersinB = [ fst clusterPairB; snd clusterPairB ]

                                let elementsToMerge =
                                    clustersinA @ clustersinB
                                    |> Seq.distinctBy (fun c -> c.Id)
                                    |> Array.ofSeq
                                    |> Array.collect (fun c -> c.Elements)

                                addClusterPairToIndex clusterPairA
                                addClusterPairToIndex clusterPairB
 
                                Merged  { Id = Guid.NewGuid(); Elements = elementsToMerge; OrderList = [||]; IndexedOrderList = new ConcurrentDictionary<Guid,int>() }
                            | _ -> NotMerged)

                [| for index in 0 .. candidatePairs.Length - 2 -> index |]
                |> Array.map (fun index -> 
                    tryMergeCompatiblePairs index
                    |> Array.choose (fun x -> match x with 
                                        | NotMerged when not (clusterExistsInIndex candidatePairs.[index]) -> 
                                            do addClusterPairToIndex candidatePairs.[index]
                                            Some (mergePair candidatePairs.[index])
                                        | Merged x -> Some x 
                                        | Duplicate -> None
                                        | _ -> None))
                |> Array.reduce (fun x acc -> x |> Array.append acc)

            let merged = 
                if mergeable.Length > 2 then 
                    (mergeCandidatePairs mergeable) 
                else
                    [| |]

            // if we still had to merge, continue merging clusters
            if merged.Count() > 0 then
                let nextBatch = merged |> Array.append allClustersWithoutMergeables
                let b = allClustersWithoutMergeables.Length
                
                sprintf "%i ms to process clusters -> %i initial clusters, %i merge candidates, %i merged, processing %i " watch.ElapsedMilliseconds clusters.Length  flattenedMergeablesSet.Length merged.Length nextBatch.Length
                |> Debug.WriteLine

                watch.Stop()
                
                nextBatch |> mergeClusters
            else
                clusters
        
        mergeClusters allClusters
        |> Array.partition (fun cluster -> cluster.Elements.Length > 1)