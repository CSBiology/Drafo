namespace NumericFilter

module NumericFilter =
    
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core
    
    type NumericFilter = 
        | IsBiggerThan of float 
        | IsSmallerThan of float 

    let initFilter filter = 
        match filter with 
        | IsBiggerThan v -> fun x -> x > v
        | IsSmallerThan v -> fun x -> x < v

    /// <summary>
    /// creates a series of the type Key,bool based on the values and the given filter
    /// </summary>
    /// <param name="filter">has the methodes IsBiggerThan and IsSmallerThan and needs a float</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="selectedCol">Column key of the column of the frame which values should be filtered</param>
    let numericFilter filter  (fp: Frame<Key,_>) (selectedCol:string)=

        let originalColumnKeys = fp.ColumnKeys

        let op = initFilter filter
        let column = 
            fp                                   
            |> getColumn<float> selectedCol
            |> transform op   
        column
    
    /// <summary>
    /// creates a frame of the type Key,bool based on the values and the given filter
    /// </summary>
    /// <param name="filter">has the methodes IsBiggerThan and IsSmallerThan and needs a float</param>
    /// <param name="fp">frame on which the operation is used</param>
    let numericFilterServeralCol filter  (fp: Frame<Key,_>) =

        let originalColumnKeys = fp.ColumnKeys
        let AllNumFilter =
            originalColumnKeys
           |> Seq.map (fun x -> numericFilter filter fp x)
        AllNumFilter   
            

