namespace RankOrderCluster

open System
open System.Collections.Concurrent
open System.Collections.Generic

module Models =

    type DataPoint = { Id : Guid; FilePath : string; Features : double[]; mutable OrderList : DataPoint array; }

    [<CustomEquality; NoComparison>]
    type Cluster = 
        { Id : Guid; Elements : DataPoint array; mutable OrderList : Cluster array; IndexedOrderList : ConcurrentDictionary<Guid, int>}
        override this.Equals(y) =
            match y with
            | :? Cluster as other -> (this.Id = other.Id)
            | _ -> false

        override x.GetHashCode() = hash x.Id

    type UncategorizedCluster = { Elements : DataPoint list }
