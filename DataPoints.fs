namespace RankOrderCluster

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Linq
open Models

module DataPoints =

    open System.Collections.Concurrent

    let getEuclidianDistance (a:double[]) (b:double[]) =
        [| for x in 0..Math.Min(a.Length, b.Length) - 1 -> x |]
        |> Array.sumBy(fun x -> Math.Pow( a.[x] - b.[x], 2.0))
        |> Math.Sqrt

    let manhattanDistanceCache = new List<Tuple<string,string>>()

    let inline getManhattanDistance (a:double[]) (b:double[]) =
        [| for x in 0..Math.Min(a.Length, b.Length) - 1 -> x |]
        |> Array.sumBy(fun x -> Math.Abs(a.[x] - b.[x]))

    let pointDistanceCache = new ConcurrentDictionary<byte[], float>(HashIdentity.Structural)

    let getCachedManhattanDistance (pointA:DataPoint) (pointB:DataPoint) =
        let cacheKey = pointA.Id.ToByteArray() |> Array.append (pointB.Id.ToByteArray())
        let similarKey = pointB.Id.ToByteArray() |> Array.append (pointA.Id.ToByteArray())

        match pointDistanceCache.ContainsKey(cacheKey) with
        | true -> Some pointDistanceCache.[cacheKey]
        | false -> match pointDistanceCache.ContainsKey(similarKey) with | true -> Some pointDistanceCache.[similarKey] | false -> None

    let storeManhattanDistanceInCache(pointA:DataPoint) (pointB:DataPoint) distance =
        pointDistanceCache.TryAdd(pointA.Id.ToByteArray().Concat(pointB.Id.ToByteArray()).ToArray(), distance)

    let getKthNearestNeighbor (subject:DataPoint) (dataSet:DataPoint array) k =
        dataSet
        |> Array.map (fun point -> 
            match getCachedManhattanDistance subject point with
            | Some cachedValue -> point, cachedValue
            | None -> 
                let distance = getManhattanDistance subject.Features point.Features
                storeManhattanDistanceInCache subject point distance |> ignore
                point, distance)
        |> Array.sortBy snd
        |> Seq.skip 1
        |> Seq.take k
        |> Seq.head
        |> fst

    let getAsymmetricRankOrderDistance (a:DataPoint) (b:DataPoint) (setA:DataPoint array) (setB:DataPoint array) =
        let positionBinA = setA |> Array.findIndex (fun elem -> elem = b)

        let pointsAtoB = setA.Take (positionBinA + 1)

        let findAndSum x =
            let elementFound = setB |> Array.tryFindIndex (fun elem -> elem = x)
            
            match elementFound with
            | Some index -> index
            | None -> 10000

        pointsAtoB
        |> Seq.sumBy (fun x -> findAndSum x)

    // Dr(a,b) === (2)
    let getRankOrderDistance (a:DataPoint) (b:DataPoint) (setA:DataPoint array) (setB:DataPoint array) =
        let positionBinA = setA |> Array.findIndex (fun elem -> elem = b)
        let positionAinB = setB |> Array.findIndex (fun elem -> elem = a)

        (getAsymmetricRankOrderDistance a b setA setB + getAsymmetricRankOrderDistance b a setB setA) / Math.Min(positionBinA, positionAinB)