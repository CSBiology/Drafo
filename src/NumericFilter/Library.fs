namespace NumericFilter

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

// module NumericFilter =
    
//     type NumericFilter = 
//         | IsBiggerThan of float 
//         | IsSmallerThan of float 

//     let initFilter filter = 
//         match filter with 
//         | IsBiggerThan v -> fun x -> x > v
//         | IsSmallerThan v -> fun x -> x < v

//     let numericFilter filter (outputDir:string) (fp:string) name =
//         let outFilePath =
//             let fileName = (name ) + ".filter"
//             Path.Combine [|outputDir;fileName|]
//         let res = KeyColumns.readAndIndexFrame fp 
//         let op = initFilter filter
//         let column = 
//             res 
//             |> getColumn<float> "Value"
//             |> transform op   
//         let toSave = seriesToFrame column
//         toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
