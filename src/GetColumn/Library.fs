namespace GetColumn

open FSharp.Stats
open Drafo
open FSharpAux
open FSharpAux.IO

module GetColumn =

    let getColumn (keyColumns:seq<KeyColumns.KeyColumns>) targetCol (outputDir:string) fp =
        let targetCol = initTargetCol targetCol
        let outFilePath =
            let fileName = targetCol + ".value"
            Path.Combine [|outputDir;fileName|]
        let keyCols = keyColumns |> Seq.map KeyColumns.initKeyCols
        let res = readAndIndexFrame keyCols fp
        let column = res |> getColumn<string> targetCol
        let toSave = seriesToFrame column
        toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
