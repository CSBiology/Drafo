namespace NumericAggregation

module NumericAggregation =
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core

    type NumericAggregation = 
        | Mean 
        | Median 
        | Sum

    let initAggregation aggregation  = 
        match aggregation with 
        | Mean      -> fun (x:seq<float>) -> Seq.mean x
        | Median    -> fun (x:seq<float>) -> Seq.median x
        | Sum       -> fun (x:seq<float>) -> Seq.sum x


    /// <summary>
    /// aggregates floats in a column that have idendical row keys, after part of the key is droped. 
    /// </summary>
    /// <param name="aggregation">has the methodes Mean, Median and Sum</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="col">Column of the frame that should be aggregated</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>
    /// <param name="filterCols">series of Key,bool that has the same keys as the frame</param>
    let numAggregat aggregation  (fp:Frame<_,_>) (col:string) (keyC:seq<string>) (filterCols:seq<Series<Key,bool>>) =

        let op = initAggregation aggregation

        let column = 
            fp
            |>getColumn col
            |>aggregate op dropAllKeyColumnsBut keyC filterCols
        column

    /// <summary>
    /// aggregates floats in a frame, column by column,that have idendical row keys, after part of the key is droped. 
    /// </summary>
    /// <param name="aggregation">has the methodes Mean, Median and Sum</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>
    /// <param name="filterCols">series of Key,bool that has the same keys as the frame</param>
    let numAgAllCol aggregation (fp:Frame<Key,_>) (keyC:seq<string>) (filterCols:seq<seq<Series<Key,bool>>>)=

        let originalColumnKeys = fp.ColumnKeys

        let colKeysForSeq seqToIt x= 
            seqToIt
            |>Seq.item x

        let forALLColl = 
            originalColumnKeys
            |> Seq.mapi (fun x i -> numAggregat aggregation fp i keyC (colKeysForSeq filterCols x))
            |> Seq.mapi (fun i x-> colKeysForSeq originalColumnKeys i, x:> ISeries<Key>)
            |>assemble
        forALLColl
            
