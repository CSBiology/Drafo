namespace NumericTransform

module NumericTransform =
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core


    type NumericTransform =
        | Log2
        | Substract of float 
        | Add of float
        | DivideBy of float
        | MultiplyBy of float 
    
    let initNumericTransform transform =
        match transform with 
        | Log2          -> log2 
        | Substract v   -> fun x -> x - v 
        | Add v         -> fun x -> v + x
        | DivideBy v    -> fun x -> x / v
        | MultiplyBy v  -> fun x -> x * v 

    /// <summary>
    /// transforms floats in a column. 
    /// </summary>
    /// <param name="numericTransform">has the methodes Log2, Substract, Add, divideBy and MultiplyBy</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="selectedCol">Column key of the column of the frame which values should be transformed</param>
    let numericTransformOneCol numericTransform  (fp:Frame<Key,_>) (selectedCol:string) =

        let op = initNumericTransform numericTransform

        let column = 
            fp 
            |> getColumn<float> selectedCol
            |> transform op 
        column

    /// <summary>
    /// transforms floats in a frame, column by column.
    /// </summary>
    /// <param name="numericTransform">has the methodes Log2, Substract, Add, divideBy and MultiplyBy</param>
    /// <param name="fp">frame on which the operation is used</param>
    let numericTransformAllCols numericTransform  (fp:Frame<Key,_>)=
        let originalColumnKeys = fp.ColumnKeys
        let colKeysForSeq seqToIt x= 
            seqToIt
            |>Seq.item x

        let forALLColl = 
            originalColumnKeys
            |> Seq.map (fun i ->numericTransformOneCol numericTransform fp i)
            |> Seq.mapi (fun i x-> colKeysForSeq originalColumnKeys i, x:> ISeries<Key>)
            |> assemble
        forALLColl
        
        