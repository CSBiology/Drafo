namespace GroupWiseNumericTransform

    


   

module GroupWiseNumericTransform =
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core


    type GroupWiseNumericTransform =
        | DivideByMedian
        | DivideByMean  
        | SubstractMedian
        | SubstractMean  
        
    let initGroupWiseNumericTransform transform =
        match transform with 
        | DivideByMedian -> 
            fun x -> 
                let m = Seq.median x
                fun x -> x / m 
        | DivideByMean   -> 
                fun x -> 
                    let m = Seq.mean x
                    fun x -> x / m
        | SubstractMedian   -> 
                fun x -> 
                    let m = Seq.median x
                    fun x -> x - m
        | SubstractMean   -> 
                fun x -> 
                    let m = Seq.mean x
                    fun x -> x - m

    /// <summary>
    /// transforms floats in a column that have idendical row keys, after part of the key is droped. 
    /// </summary>
    /// <param name="transform">has the methodes DivideByMean, DivideByMedian, SubtractMean and SubtractMedian</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="selectedCol">Column key of the column of the frame which values should be transformed</param>
    let groupWiseNumericTransform transform (keyC:seq<string>)  (fp: Frame<Key,_>) (selectedCol:string) =

        let op = initGroupWiseNumericTransform transform

        let column = 
            fp 
            |> getColumn<float> selectedCol
            |> groupTransform op dropAllKeyColumnsBut keyC
        column
    
    /// <summary>
    /// transforms floats in a frame, column by column,that have idendical row keys, after part of the key is droped. 
    /// </summary>
    /// <param name="transform">has the methodes DivideByMean, DivideByMedian, SubtractMean and SubtractMedian</param>
    /// <param name="keyC">seq of string containing the first part of Keys that shall not be dropped</param>
    /// <param name="fp">frame on which the operation is used</param>
    let groupWiseNumericTransformAllCols  transform (keyC:seq<string>) (fp:Frame<Key,_>)  =
        
        let originalColumnKeys = fp.ColumnKeys

        let colKeysForSeq seqToIt x= 
            seqToIt
            |>Seq.item x

        let forALLColl = 
            originalColumnKeys
            |> Seq.map (fun i ->groupWiseNumericTransform transform keyC fp i)
            |> Seq.mapi (fun i x-> colKeysForSeq originalColumnKeys i, x:> ISeries<Key>)
            |> assemble
        forALLColl
        

