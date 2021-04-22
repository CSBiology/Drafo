namespace GroupWiseNumericTransform

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
// module GroupWiseNumericTransform =
    
//     type GroupWiseNumericTransform =
//         | DivideByMedian
//         | DivideByMean  
//         | SubstractMedian
//         | SubstractMean  
        
//     let initGroupWiseNumericTransform transform =
//         match transform with 
//         | DivideByMedian -> 
//             fun x -> 
//                 let m = Seq.median x
//                 fun x -> x / m 
//         | DivideByMean   -> 
//                 fun x -> 
//                     let m = Seq.mean x
//                     fun x -> x / m
//         | SubstractMedian   -> 
//                 fun x -> 
//                     let m = Seq.median x
//                     fun x -> x - m
//         | SubstractMean   -> 
//                 fun x -> 
//                     let m = Seq.mean x
//                     fun x -> x - m

//     let groupWiseNumericTransform transform (keyColumns:seq<KeyColumns.KeyColumns>) (outputDir:string) (fp:string) name =
//         let outFilePath =
//             let fileName = (name ) + ".value"
//             Path.Combine [|outputDir;fileName|]
//         let res = KeyColumns.readAndIndexFrame fp 
//         let op = initGroupWiseNumericTransform transform
//         let groupKeyCols = keyColumns |> Seq.map KeyColumns.initKeyCols |> Seq.map (fun x -> x.ColumnName)
//         let column = 
//             res 
//             |> getColumn<float> "Value"
//             |> groupTransform op groupKeyCols  
//         let toSave = seriesToFrame column
//         toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   