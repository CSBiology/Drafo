#r "nuget: Deedle, 2.5.0" 
#r "nuget: FSharp.Stats, 0.4.3" 
#r "nuget: FSharpAux, 1.1.0" 
#r "nuget: FSharpAux.IO, 1.1.0" 
#r "nuget: DynamicObj, 1.0.1" 



#load "DrafoCore.fs"
#load "GroupFilter.fs"
#load "GroupWiseNumericTransform.fs"
#load "NumericFilter.fs"
#load "NummericAggregation.fs"
#load "StringAggregation.fs"







open Deedle
open GroupFilter.GroupFilter
open GroupWiseNumericTransform.GroupWiseNumericTransform
open Drafo.Core
open FSharp.Stats
open FSharpAux
open NumericAggregation.NumericAggregation
open NumericFilter.NumericFilter
open StringAggregation.StringAggregation



do fsi.AddPrinter(fun (printer:Deedle.Internal.IFsiFormattable) -> "\n" + (printer.Format()))


let testFrame = 
    frame [
        ("Column1")=> series ["row1" => 1;"row2" =>3]
        ("Column2")=> series ["row1" => 10;"row2" =>100]    
    ]
    |>Frame.addCol "keys"(series ["row1" => "key1" ;"row2" =>"key2"])




let transformedFrame = indexWithColumnValues ["keys"] testFrame












let testFrameTwo = 
    frame [
        ("Column1")=> series ["row1" => 1;"row2" =>3]
        ("Column2")=> series ["row1" => 10;"row2" =>100]    
    ]
    |>Frame.addCol "keys"(series ["row1" => "key1" ;"row2" =>"key1"])
    |>Frame.addCol "KeyPartTwo"(series ["row1" => "key2" ;"row2" =>"key3"])

let transformedTestFrameTwo = indexWithColumnValues ["keys";"KeyPartTwo"] testFrameTwo








