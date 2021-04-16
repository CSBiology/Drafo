namespace Drafo

module Core =
    open Deedle
    open DynamicObj
    open System.Collections.Generic

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

    ///
    let indexWithColumnValues (keyCols:(seq<string>) ) (f:Frame<_,string>) :Frame<Key,_>= 
        f
        |> Frame.indexRowsUsing (fun s -> 
                keyCols
                |> Seq.fold (fun key x -> 
                    let value = s.GetAs<string>(x) 
                    key.addCol (x,value)
                    ) (Key())
            )  

    ///
    let readFrame fp = Frame.ReadCsv(fp,hasHeaders=true,inferTypes=false,separators="\t")
      
    ///
    let readAndIndexFrame keyCols fp = 
        readFrame fp
        |> indexWithColumnValues keyCols

    ///
    let inline getColumn<'a> column (f:Frame<Key, string> )  =
        f.GetColumn<'a>(column)

    ///
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

    ///
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

    ///
    let createFilter (op : 'a -> bool) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    ///
    let transform (op : 'a -> 'b) (s: Series<'KeyType, 'a>) = 
        s
        |> Series.mapValues op

    ///
    let zip (op : 'a -> 'a -> 'b) (s1: Series<'KeyType, 'a>) (s2: Series<'KeyType, 'a>) = 
        Series.zipInner s1 s2
        |> Series.mapValues (fun (s1,s2) -> 
            op s1 s2
            )

    ///
    let dropAllPropertiesBut (properties:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> properties |> Seq.contains x.Key )
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    ///
    let dropProperties (properties:seq<string>) (key:Key) = 
        let newK = Key()
        key.GetProperties true
        |> Seq.filter (fun x -> properties |> Seq.contains x.Key |> not)
        |> Seq.fold (fun (k:Key) x -> k.addCol (x.Key,x.Value) ) newK

    ///
    let groupTransform (op :'a [] -> 'a -> 'b) (newKeys:seq<string>) (s: Series<'KeyType, 'a>) =
        s
        |> Series.groupBy (fun k v -> dropAllPropertiesBut newKeys k )
        |> Series.mapValues (fun valueCol -> 
            let fInit = valueCol |> Series.values |> Array.ofSeq |> op
            let filterCol = 
                valueCol
                |> Series.mapValues fInit
            filterCol 
        )
        |> Series.values
        |> Series.mergeAll

    ///
    let createGroupFilter (op :'a [] -> 'a -> bool) (newKeys:seq<string>) (s: Series<'KeyType, 'a>) =
        groupTransform op newKeys s

    ///
    let aggregate (op : seq<'a> -> 'b) (newKeys:seq<string>) (filters:seq<Series<Key,bool>>) (col:Series<Key,'a>) = 
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
        |> Frame.applyLevel (dropAllPropertiesBut newKeys) (fun s -> s |> Series.values |> op)
        |> Frame.getCol colID

    ///
    let assemble (cols:seq<(string * #ISeries<Key>)>) =
        Frame.ofColumns cols

    ///
    let pivot (pivotCol:string) (assembeledFrame:Frame<Key,string>) =
        assembeledFrame
        |> Frame.nestBy (fun k -> 
            let value: string option = k.TryGetTypedValue pivotCol
            value.Value
            )
        |> Series.map (fun k f ->
                f
                |> Frame.mapColKeys (fun ck -> sprintf "%s.%s" k ck)
                |> Frame.mapRowKeys (dropProperties [pivotCol])
            )
        |> Series.values
        |> Frame.mergeAll