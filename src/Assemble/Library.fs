namespace Assemble

open FSharp.Stats
open Deedle
open Drafo.Core
open FSharpAux
open FSharpAux.IO
open System.IO

module GetColumn =

    ///
    let getColumn (keyColumns:seq<string>) targetCol (outputDir:string) fp =
        let outFilePath =
            let fileName = targetCol + ".value"
            Path.Combine [|outputDir;fileName|]
        let res = readAndIndexFrame keyColumns fp
        let column = res |> getColumn<string> targetCol
        let toSave = seriesToFrame column
        toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
