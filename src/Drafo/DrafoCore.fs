namespace Drafo

module Core =
    open Deedle
    open DynamicObj



    type Key() = 
        inherit DynamicObj ()
       
        member this.addCol(columns:(string*'a)) =
            this.SetValue columns
            this  

        override this.ToString() = 
            let sb = new System.Text.StringBuilder()
            // sb.Append
            (this.GetProperties true)        
            |> Seq.iter (fun x -> 
                let value = x.Value.ToString() 
                sb.AppendLine(x.Key+": "+value)
                |> ignore
                )
            (sb.ToString())

        override this.Equals(b) =
            match b with
            | :? Key as p -> 
                let propA = (this.GetProperties true) |> Seq.map (fun x -> x.Key,x.Value) 
                let propB = (p.GetProperties true) |> Seq.map (fun x -> x.Key,x.Value) 
                Seq.map2 (fun (x1,x2) (y1,y2) -> x1 = unbox y1 && x2 = unbox y2) propA propB 
                |> Seq.contains false
                |> not
            | _ -> false

        override this.GetHashCode() = 
            let sb = new System.Text.StringBuilder()
            // sb.Append
            (this.GetProperties true)        
            |> Seq.iter (fun x -> 
                let value = x.Value.ToString() 
                sb.Append(value)
                |> ignore
                )
            (sb.ToString())
            |> hash   

    /// <summary>
    /// index a frame by selecting the columns that are used to create  row keys of type Key 
    /// </summary>
    /// <param name="keyCols">columns containing strings that are used for Key creation</param>
    /// <param name="f">frame which is indexed</param>
    let indexWithColumnValues (keyCols:(seq<string>) ) (f:Frame<_,string>) :Frame<Key,_>= 
        f
        |> Frame.indexRowsUsing (fun s -> 
                keyCols
                |> Seq.fold (fun key x -> 
                    let value = s.GetAs<string>(x) 
                    key.addCol (x,value)
                    ) (Key())
            )

    /// <summary>
    /// read external csv as frame does not infer types
    /// </summary>
    /// <param name="fp">path to the csv that you want to read in</param>
    let readFrame fp = Frame.ReadCsv(fp,hasHeaders=true,inferTypes=false,separators="\t") 

    /// <summary>
    /// a combination of readFrame and index withColumnValues keep in mind that the columns needed for indexing should already be present in the frame
    /// </summary>
    /// <param name="keyCols">columns containing strings that are used for Key creation</param>
    /// <param name="fp">path to the csv that you want to read in</param>
    let readAndIndexFrame keyCols fp = 
        readFrame fp
        |> indexWithColumnValues keyCols///

    /// <summary>
    /// Drafo version of GetColumn. get a column of a frame based on the column key
    /// </summary>
    /// <param name="column">string that represents the Column key</param>
    /// <param name="f">frame which contains the column </param>
    let inline getColumn<'a> column (f:Frame<Key, string> )  =
        f.GetColumn<'a>(column)

    ///makes a series into a frame the row Key objeckts get transfered to columns the row index are ints afterwards
    /// <summary>
    /// Makes a series of type <Key,_> into a frame of type <int,_>. The row Key objects get transfered to columns the row index are ints afterwards
    /// </summary>
    /// <param name="s">series that is turned into a frame</param>
    let inline seriesToFrame (s: Series<Key, 'a>) =
        s
        |> Series.map (fun k s -> 
            (k.GetProperties true) 
            |> Seq.map (fun x -> x.Key,x.Value)
            |> Series.ofObservations
        )    
        |> Frame.ofRows
        |> Frame.addCol "Value" s
        |> Frame.indexRowsOrdinally   

    /// <summary>
    /// Changes the row keys from type Key to int un transfers the Key into columns (reverse indexWithColumnValues)
    /// </summary>
    /// <param name="f">frame which contains the column </param>
    let inline rowKeyToColumns (f: Frame<Key, string>) =
        let rowKeysAsColumns = 
            f
            |> Frame.mapRows (fun k s -> 
                (k.GetProperties true) 
                |> Seq.map (fun x -> x.Key,x.Value)
                |> Series.ofObservations
            )    
            |> Frame.ofRows
        Frame.join JoinKind.Inner rowKeysAsColumns f 
        |> Frame.indexRowsOrdinally

    /// <summary>
    /// turns values in a given series to into bools based on the function used
    /// </summary>
    /// <param name="op">function that determines what bool the value is </param>
    /// <param name="s">series on which the function is applied</param>
    let createFilter (op : 'a -> bool) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    /// <summary>
    /// transform values in a series with a function
    /// </summary>
    /// <param name="op">function that determines how the value is transformed </param>
    /// <param name="s">series on which the function is applied</param>
    let transform (op : 'a -> 'b) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    /// <summary>
    /// zips two series based on the given function.
    /// </summary>
    /// <param name="op">function that determines how the sereies are zipped. This function does need to take two series as parameter </param>
    /// <param name="s1">series1 on which the function is applied</param>
    /// <param name="s2">series2 on which the function is applied</param>
    let zip (op : 'a -> 'a -> 'b) (s1: Series<'KeyType, 'a>) (s2: Series<'KeyType, 'a>) = 
        Series.zipInner s1 s2
        |> Series.mapValues (fun (s1,s2) -> 
            op s1 s2
            )

    /// <summary>
    /// drops row keys of the type Key that are not given in the seq of string ( you only have to give the Key part of the Key value pair
    /// </summary>
    /// <param name="keyColumns">seq of string containing all the first parts of Keys that should not be dropped</param>
    /// <param name="key"> the row keys of type Key </param>
    let dropAllKeyColumnsBut (keyColumns:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> keyColumns |> Seq.contains x.Key )
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    /// <summary>
    /// drops row keys of the type Key that are given in the seq of string ( you only have to give the Key part of the Key value pair
    /// </summary>
    /// <param name="keyColumns">seq of string containing all the first parts of Keys that should be dropped</param>
    /// <param name="key"> the row keys of type Key </param>
    let dropKeyColumns (keyColumns:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> keyColumns |> Seq.contains x.Key |> not)
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    /// <summary>
    /// a version of transform that works on groups in a series dropKeyColumns and dropAllKeyColumnsBut can be used for the parameter modifyKeyColumns
    /// </summary>
    /// <param name="op">function that takes a seq/array and a parameter of the same type and changes the parameter</param>
    /// <param name="modifyKeyColumns">modifey Key columns for example dropAllKeyColumnsBut</param>
    /// <param name="keyColumns">seq of string containing the first part of Keys for the modifyKeyColumns parameter</param>
    /// <param name="s">series on which the operations happen</param>
    let groupTransform (op :'a [] -> 'a -> 'b) modifyKeyColumns (keyColumns:seq<string>) (s: Series<'KeyType, 'a>) =
        s
        |> Series.groupBy (fun k v -> modifyKeyColumns keyColumns k )
        |> Series.mapValues (fun valueCol -> 
            let fInit = valueCol |> Series.values |> Array.ofSeq |> op
            let filterCol = 
                valueCol
                |> Series.mapValues fInit
            filterCol 
        )
        |> Series.values
        |> Series.mergeAll

    /// <summary>
    /// a version of createFilter that works on groups in a series dropKeyColumns and dropAllKeyColumnsBut can be used for the parameter modifyKeyColumns
    /// </summary>
    /// <param name="op">function that takes a seq/array and a parameter of the same type and returns a bool</param>
    /// <param name="modifyKeyColumns">modifey Key columns for example dropAllKeyColumnsBut</param>
    /// <param name="keyColumns">seq of string containing the first part of Keys for the modifyKeyColumns parameter</param>
    /// <param name="s">series on which the operations happen</param>
    let createGroupFilter (op :'a [] -> 'a -> bool) modifyKeyColumns (keyColumns:seq<string>) (s: Series<'KeyType, 'a>) =
        groupTransform op modifyKeyColumns keyColumns s

    /// <summary>
    /// aggregates values if the Keys ar identical in a series dropKeyColumns and dropAllKeyColumnsBut can be used for the parameter modifyKeyColumns
    /// </summary>
    /// <param name="op">function that takes a seq und returns a singel value examples are mean and median</param>
    /// <param name="modifyKeyColumns">modifey Key columns for example dropAllKeyColumnsBut</param>
    /// <param name="keyColumns">seq of string containing the first part of Keys for the modifyKeyColumns parameter</param>
    /// <param name="filters">series of Key,bool that has the same keys as col</param>
    /// <param name="col">series on which the operations happen</param>
    let aggregate (op : seq<'A> -> 'C) modifyKeyColumns (keyColumns:seq<string>) (filters:seq<Series<Key,bool>>) (col:Series<Key,'A>)  :Series<Key,'C> = 
        let filtered = 
            filters
            |> Seq.map (fun s -> 
                System.Guid.NewGuid(),
                s |> Series.filterValues id)
            |> Series.ofObservations
            |> Frame.ofColumns
            |> Frame.dropSparseRows
        let colID = (System.Guid.NewGuid())
        filtered
        |> Frame.addCol colID col
        |> Frame.dropSparseRows
        |> Frame.getCol colID
        |> Series.applyLevel (modifyKeyColumns keyColumns) (Series.values >> op)

    /// <summary>
    /// turn series into frames while keeping the Key objects.
    /// </summary>
    /// <param name="cols">#ISeries Key, can be cast from a series of Key,_,  which is turned into a frame</param>
    let assemble (cols:seq<(string * #ISeries<Key>)>) =
        Frame.ofColumns cols

    /// <summary>
    /// pivot the Key, given as a string, of the given frame 
    /// </summary>
    /// <param name="pivotCol">string that denotes which Key object is used for pivoting </param>
    /// <param name="assembeledFrame">frame on which the operation happens</param>
    let pivot (pivotCol:string) (assembeledFrame:Frame<Key,string>) =
        assembeledFrame
        |> Frame.nestBy (fun k -> 
            let value: string option = k.TryGetTypedValue pivotCol
            value.Value
            )
        |> Series.map (fun k f ->
                f
                |> Frame.mapColKeys (fun ck -> sprintf "%s.%s" k ck)
                |> Frame.mapRowKeys (dropKeyColumns [pivotCol])
            )
        |> Series.values
        |> Frame.mergeAll