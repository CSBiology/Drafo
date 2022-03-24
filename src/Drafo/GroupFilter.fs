namespace GroupFilter

module GroupFilter =
    
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core

    
    type GroupFilter = 
        | Tukey of float  
        | Stdev of float

    
    let initGroupFilter gf =
        match gf with 
        | Tukey threshold -> 
            fun values -> 
                let tukey = FSharp.Stats.Signal.Outliers.tukey threshold
                match tukey values with 
                | Intervals.Interval.ClosedInterval (lower, upper) -> 
                    (fun v -> v <= upper && v >= lower)
                | _ -> fun v -> false
        | Stdev threshold ->  
            fun values -> 
                let mean = Seq.mean values
                let stdev = Seq.stDev values
                (fun v -> v <= (mean+stdev*threshold) && v >= (mean-stdev*threshold)
                ) 
    /// <summary>
    /// creates a series of the type Key,bool based on the values and the given filter. The rowKeys are the same as the input frame
    /// </summary>
    /// <param name="filter">has the methodes Tukey and Stdev and needs a float</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>
    /// <param name="selectedCol">Column key of the column of the frame which values should be filtered</param>
    let groupFilter filter  (fp:Frame<Key,_>) (keyC:seq<string>) (selectedCol:string)  =
        let op = initGroupFilter filter
        
        let column = 
            fp
            |> getColumn<float> selectedCol
            |> groupTransform op dropAllKeyColumnsBut keyC 
              
        column

    /// <summary>
    /// creates a series of the type Key,bool based on the values and the given filter. The rowKeys are the same as the input frame
    /// </summary>
    /// <param name="filter">has the methodes Tukey and Stdev and needs a float</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>     
    let groupFilterAllCol filter (fp:Frame<Key,_>) (keyC:seq<string>) =

        let originalColumnKeys = fp.ColumnKeys
        let AllNumFilter =
            originalColumnKeys
           |> Seq.map (fun x -> groupFilter filter fp keyC x)
        AllNumFilter 

