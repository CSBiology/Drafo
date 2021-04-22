namespace NumericTransform

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

module NumericTransform =
    
    type NumericTransform =
        | Log2
        | Substract of float 
        | Add of float
        | DivideBy of float
        | MultiplyBy of float 
        
    let initNumericTransform transform =
        match transform with 
        | Log2          -> log2 
        | Substract v   -> fun x -> x - v 
        | Add v         -> fun x -> v + x
        | DivideBy v    -> fun x -> x / v
        | MultiplyBy v  -> fun x -> x * v 

    let numericTransform numericTransform (outputDir:string) (fp:string) name =
        let outFilePath =
            let fileName = (name ) + ".value"
            Path.Combine [|outputDir;fileName|]
        let res = KeyColumns.readAndIndexFrame fp 
        let op = initNumericTransform numericTransform
        let column = 
            res 
            |> getColumn<float> "Value"
            |> transform op   
        let toSave = seriesToFrame column
        toSave.SaveCsv(outFilePath,separator='\t',includeRowKeys=false)   
