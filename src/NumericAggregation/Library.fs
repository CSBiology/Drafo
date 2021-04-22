namespace NumericAggregation

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
// module NumericAggregation =
    
//     type NumericAggregation = 
//         | Mean 
//         | Median 
//         | Sum

//     let initAggregation aggregation  = 
//         match aggregation with 
//         | Mean      -> fun (x:seq<float>) -> Seq.mean x
//         | Median    -> fun (x:seq<float>) -> Seq.median x
//         | Sum       -> fun (x:seq<float>) -> Seq.sum x


//     let aggregate aggregation (keyColumns:seq<KeyColumns.KeyColumns>) (outputDir:string) (filters:seq<string>) (fp:string) name =
//         let outFilePath =
//             let fileName = (name ) + ".value"
//             Path.Combine [|outputDir;fileName|]
//         let op = initAggregation aggregation
//         let valueCol = KeyColumns.readAndIndexFrame fp |> getColumn<float> "Value"
//         let filterCols = filters |> Seq.map KeyColumns.readAndIndexFrame |> Seq.map (getColumn<bool> "Value")
//         let groupKeyCols = keyColumns |> Seq.map KeyColumns.initKeyCols |> Seq.map (fun x -> x.ColumnName)
//         let column = 
//             valueCol
//             |> aggregate op groupKeyCols filterCols  
//         let toSave = seriesToFrame column
//         toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
