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
    /// Index a frame by selecting the columns that are used to create  row keys of type `<Key>`.
    /// </summary>
    /// <param name="keyCols">Columns containing strings that are used for `<Key>` creation.</param>
    /// <param name="f">The frame that is indexed.</param>
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
    /// Read external csv as frame, does not infer types.
    /// </summary>
    /// <param name="fp">Path to the csv that you want to read in.</param>
    let readFrame fp = Frame.ReadCsv(fp,hasHeaders=true,inferTypes=false,separators="\t") 

    /// <summary>
    /// A combination of `readFrame` and `indexWithColumnValues`, keep in mind that the columns needed for indexing should already be present in the frame.
    /// </summary>
    /// <param name="keyCols">Columns containing strings that are used for `<Key>` creation.</param>
    /// <param name="fp">Path to the csv that you want to read in.</param>
    let readAndIndexFrame keyCols fp = 
        readFrame fp
        |> indexWithColumnValues keyCols///

    /// <summary>
    /// Drafo version of GetColumn. Get a column of a frame based on the column key.
    /// </summary>
    /// <param name="column">A string that represents the Column key.</param>
    /// <param name="f">The frame which contains the column. </param>
    let inline getColumn<'a> column (f:Frame<Key, string> )  =
        f.GetColumn<'a>(column)

    /// <summary>
    /// Makes a series of type `<Key,_>` into a frame of type `<int,_>`. The row Key objects get transfered to columns and the row indexes are integers afterwards.
    /// </summary>
    /// <param name="s">The series that is turned into a frame.</param>
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
    /// Changes the row keys from type `<Key>` to int and transfers the `<Key>` into columns (the reverse of `indexWithColumnValues`)
    /// </summary>
    /// <param name="f">Frame which contains the column. </param>
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
    /// Turns values in each series into bools based on the function that is used.
    /// </summary>
    /// <param name="op">The function that determines what bool the value turns into.</param>
    /// <param name="s">The series on which the function is applied. </param>
    let createFilter (op : 'a -> bool) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    /// <summary>
    /// Transform values in a series with a given function.
    /// </summary>
    /// <param name="op">The function that determines how the value is transformed </param>
    /// <param name="s">The series on which the function is applied. </param>
    let transform (op : 'a -> 'b) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    /// <summary>
    /// Zip two series based on the given function.
    /// </summary>
    /// <param name="op">The function that determines how the sereies are zipped. This function does need to take two series as parameter. </param>
    /// <param name="s1">The first series on which the function is applied.</param>
    /// <param name="s2">The second series on which the function is applied.</param>
    let zip (op : 'a -> 'a -> 'b) (s1: Series<'KeyType, 'a>) (s2: Series<'KeyType, 'a>) = 
        Series.zipInner s1 s2
        |> Series.mapValues (fun (s1,s2) -> 
            op s1 s2
            )

    /// <summary>
    /// Drops row keys of the type `<Key>` that are not given in the sequence of string (you only have to give the first part of the `<Key>`).
    /// </summary>
    /// <param name="keyColumns">A sequence of string containing all the first parts of `<Key>` that should not be dropped. </param>
    /// <param name="key"> The row keys of type `<Key>`, from which keys can be dropped. </param>
    let dropAllKeyColumnsBut (keyColumns:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> keyColumns |> Seq.contains x.Key )
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    /// <summary>
    /// Drops row keys of the type `<Key>` that are given in the sequence of string (you only have to give the first part of the `<Key>`).
    /// </summary>
    /// <param name="keyColumns">A sequence of string containing all the first parts of `<Key>` that should be dropped. </param>
    /// <param name="key"> The row keys of type `<Key>`, from which keys can be dropped. </param>
    let dropKeyColumns (keyColumns:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> keyColumns |> Seq.contains x.Key |> not)
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    /// <summary>
    /// A version of `transform` that works on groups in a series. `dropKeyColumns` and `dropAllKeyColumnsBut` can be used for the parameter modifyKeyColumns
    /// </summary>
    /// <param name="op">Function that takes a seq/array and a parameter of the same type and changes the parameter, based on op.</param>
    /// <param name="modifyKeyColumns">Modify `<Key>` columns for example with `dropAllKeyColumnsBut`. </param>
    /// <param name="keyColumns">A sequence of string, containing the first part of `<Key>`s for the modifyKeyColumns parameter.</param>
    /// <param name="s">The series on which the operations happen</param>
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
    /// A version of `createFilter` that works on groups in a series. `dropKeyColumns` and `dropAllKeyColumnsBut` can be used for the parameter modifyKeyColumns.
    /// </summary>
    /// <param name="op">Function that takes a seq/array and a parameter of the same type and changes the parameter into a bool, based on op.</param>
    /// <param name="modifyKeyColumns">Modify `<Key>` columns for example with `dropAllKeyColumnsBut`. </param>
    /// <param name="keyColumns">A sequence of string, containing the first part of `<Key>`s for the modifyKeyColumns parameter. </param>
    /// <param name="s">The series on which the operations happen</param>
    let createGroupFilter (op :'a [] -> 'a -> bool) modifyKeyColumns (keyColumns:seq<string>) (s: Series<'KeyType, 'a>) =
        groupTransform op modifyKeyColumns keyColumns s

    /// <summary>
    /// Aggregates values if the  `<Key>` are identical in a series. `dropKeyColumns` and `dropAllKeyColumnsBut` can be used for the parameter modifyKeyColumns.
    /// </summary>
    /// <param name="op">Function that takes a sequence und returns a single value, examples are mean and median</param>
    /// <param name="modifyKeyColumns">Modify `<Key>` columns for example with `dropAllKeyColumnsBut`. </param>
    /// <param name="keyColumns">A sequence of string, containing the first part of `<Key>`s for the modifyKeyColumns parameter.</param>
    /// <param name="filters">A series of `<Key,bool>` that has the same `<Key>`s as col.</param>
    /// <param name="col">The series on which the operations happen</param>
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
    /// Turn a series into frames, while keeping the Key objects.
    /// </summary>
    /// <param name="cols">#ISeries `<Key>`, can be cast from a series of `<Key,_>`,  which is turned into a frame.</param>
    let assemble (cols:seq<(string * #ISeries<Key>)>) =
        Frame.ofColumns cols

    /// <summary>
    /// Pivots the `<Key>`, given as a string, of the given frame. 
    /// </summary>
    /// <param name="pivotCol">A string that denotes which `<Key>` object is used for pivoting </param>
    /// <param name="assembeledFrame">The frame on which the operation happens</param>
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
