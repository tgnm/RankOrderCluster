namespace RankOrderCluster

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq

open Models
open DataPoints

module Clusters =
    
    let getAsymmetricClusterDistance (clusterA:Cluster) (clusterB:Cluster) =

        let positionBinA = clusterA.OrderList |> Array.findIndex (fun x -> x.Id = clusterB.Id)

        let pointsAtoB = clusterA.OrderList.Take (positionBinA)

        let findAndSum x =
            clusterB.IndexedOrderList.[x.Id]

        pointsAtoB
        |> Seq.sumBy (fun x -> findAndSum x)

    // d(Ci, Cj) === (3)
    let getClusterLevelAbsoluteDistance (clusterA:Cluster) (clusterB:Cluster) =
        clusterA.Elements
        |> Array.map (fun elemA -> 
            clusterB.Elements
            |> Array.map (fun elemB ->
                match DataPoints.getCachedManhattanDistance elemA elemB with
                | Some cachedValue -> cachedValue
                | None -> 
                    let distance = DataPoints.getManhattanDistance elemA.Features elemB.Features
                    DataPoints.storeManhattanDistanceInCache elemA elemB distance |> ignore
                    distance))
        |> Array.collect (fun x -> x)
        |> Array.min

    let getClusterOrderList (clusterX:Cluster) (allClusters:Cluster array)=
        
        clusterX.IndexedOrderList.Clear()
        
        allClusters
        |> Array.filter (fun cluster -> not (cluster.Id.Equals(clusterX.Id)))
        |> Array.sortBy (fun cluster -> getClusterLevelAbsoluteDistance cluster clusterX)
        |> fun x ->
            Array.iteri (fun i elem -> clusterX.IndexedOrderList.TryAdd(elem.Id, i) |> ignore) x |> ignore
            x
        |> Array.ofSeq

    // Dr(Ci, Cj) === (4)
    let getClusterLevelRankOrderDistance (clusterA:Cluster) (clusterB:Cluster) (allClusters:Cluster array) =

        let indexOfBInOrderListA = clusterA.OrderList |> Array.findIndex (fun item -> item.Id = clusterB.Id) |> float
        let indexOfAinOrderListB = clusterB.OrderList |> Array.findIndex (fun item -> item.Id = clusterA.Id) |> float

        let distanceBInOrderListA = getAsymmetricClusterDistance clusterB clusterA |> float
        let distanceAInOrderListB = getAsymmetricClusterDistance clusterA clusterB |> float

        let minimalDistance = Math.Min(indexOfBInOrderListA, indexOfAinOrderListB)

        let rankOrderDistance = (distanceBInOrderListA + distanceAInOrderListB) / Math.Max(minimalDistance, 1.0)

        rankOrderDistance
    
    // for debugging only
    let mutable minDistance : double = 10000.0
    let mutable minLowestSecondPart : double = 10000.0
    let mutable minLowestPart : double = 10000.0
    let mutable minAbsoluteDistance : double = 10000.0

    // Dn (Ci, Cj) === (5)
    let getClusterLevelNormalizedDistance (clusterA:Cluster) (clusterB:Cluster) (allDataPoints:DataPoint array) K =
        
        let calculateKNearestDistance (element:DataPoint) (k:int) = 
            let kNearest = DataPoints.getKthNearestNeighbor element allDataPoints k
            
            let distance = match DataPoints.getCachedManhattanDistance element kNearest with
                | Some cachedValue -> cachedValue
                | None -> 
                    let distance = DataPoints.getManhattanDistance element.Features kNearest.Features
                    DataPoints.storeManhattanDistanceInCache element kNearest distance |> ignore
                    distance

            distance * (1.0 / (float k))
        
        let sumKNearestDistances (element:DataPoint) = 
            [| for i in 1 .. K -> i |]
            |> Array.sumBy (fun k -> calculateKNearestDistance element k)
                    
        let lowestSecondPart = 
            clusterA.Elements |> Array.append clusterB.Elements
            |> Array.sumBy (fun element -> 
                let sumOfDistances = sumKNearestDistances element
                (1.0 / (float K)) * sumOfDistances)

        minLowestSecondPart <- Math.Min(minLowestSecondPart, lowestSecondPart)

        System.Diagnostics.Debug.WriteLine((sprintf "Minimum lowest second part found so far: %f" minLowestSecondPart))
            
        let lowestPart = 
            lowestSecondPart * (1.0 / ((float clusterA.Elements.Length) + (float clusterB.Elements.Length)))

        minLowestPart <- Math.Min(minLowestPart, lowestPart)

        System.Diagnostics.Debug.WriteLine((sprintf "Minimum lowest part found so far: %f" minLowestPart))

        let absoluteDistance = getClusterLevelAbsoluteDistance clusterA clusterB

        minAbsoluteDistance <- Math.Min(minAbsoluteDistance, absoluteDistance)

        System.Diagnostics.Debug.WriteLine((sprintf "Minimum absolute distance found so far: %f" minAbsoluteDistance))

        let normalizedDistance = (1.0 / lowestPart) * absoluteDistance

        minDistance <- Math.Min(minDistance, normalizedDistance)

        System.Diagnostics.Debug.WriteLine((sprintf "Minimum normalized distance found so far: %f" minDistance))

        normalizedDistance