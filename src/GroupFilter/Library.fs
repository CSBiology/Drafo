namespace GroupFilter

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

// module GroupFilter =
    
//     ///
//     type GroupFilter = 
//         | Tukey of float  
//         | Stdev of float

//     ///
//     let initGroupFilter gf =
//         match gf with 
//         | Tukey threshold -> 
//             fun values -> 
//                 let tukey = FSharp.Stats.Signal.Outliers.tukey threshold
//                 match tukey values with 
//                 | Intervals.Interval.ClosedInterval (lower, upper) -> 
//                     (fun v -> v <= upper && v >= lower)
//                 | _ -> fun v -> false
//         | Stdev threshold ->  
//             fun values -> 
//                 let mean = Seq.mean values
//                 let stdev = Seq.stDev values
//                 (fun v -> v <= (mean+stdev*threshold) && v >= (mean-stdev*threshold)
//                 ) 

//     let groupFilter filter (keyColumns:seq<KeyColumns.KeyColumns>) (outputDir:string) (fp:string) name =
//         let outFilePath =
//             let fileName = (name ) + ".filter"
//             Path.Combine [|outputDir;fileName|]
//         let res = KeyColumns.readAndIndexFrame fp 
//         let op = initGroupFilter filter
//         let groupKeyCols = keyColumns |> Seq.map KeyColumns.initKeyCols |> Seq.map (fun x -> x.ColumnName)
//         let column = 
//             res 
//             |> getColumn<float> "Value"
//             |> groupTransform op groupKeyCols  
//         let toSave = seriesToFrame column
//         toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)  