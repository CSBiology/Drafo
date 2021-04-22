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
        
// module Assemble =

//     let assemble (outputDir:string) (cols:seq<string>) name =
//         let outFilePath =
//             let fileName = name + ".txt"
//             Path.Combine [|outputDir;fileName|]
//         let filterCols = cols |> Seq.map KeyColumns.readAndIndexFrame |> Seq.map (getColumn<string> "Value")
//         let filterColsNamed = Seq.map2 (fun (x:string) y -> (Path.GetFileNameWithoutExtension x),y) cols filterCols
//         let assembeled = filterColsNamed |> assemble    
//         let toSave = assembeled |>  rowKeyToColumns 
//         toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
