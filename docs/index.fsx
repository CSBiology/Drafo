(***hide***)

#r "nuget: Deedle, 2.5.0"
#r "nuget: FSharp.Stats, 0.4.3" 
#r "nuget: FSharpAux, 1.1.0" 

#r "nuget: DynamicObj, 1.0.1" 
#r "../bin/Drafo/netstandard2.0/Drafo.dll"
open Deedle
open FSharp.Stats
open FSharpAux

open Drafo.Core
open NumericAggregation.NumericAggregation
(**
# Drafo documentation

This library adds a DynamicObject called <Key> that can be used as row key of Deedle frames and series. 
The functions in this library all either create frames with <Key> objects or work with frames or series that have <Key> objects. 
The modules allow for easy creation of filters und processing of either series or frames.

#### Table of contents 

- [Installation](#Installation)
- [Usage](#Usage)
- [Example](#Example)

## Installation

This library is available as nuget package(from [nuget]()):

## Usage

Add the NuGet package as reference and open Drafo/Drafo.Core and any module you want to use. 
Because Drafo is build on Deedle it is highly recommended to use Deedle too.

You can create your own frame, with <Key> objects as row keys, transform existing frames into frames that have <Key> objects 
or read in csv files and then transform them. 

Then one can use the functions in the Drafo core or the modules to manipulate series or frames that have <Key> objects as row keys.

## Example 

We have a frame of floats that we want to use some operations of the Drafo library on. So first we must index it, 
for that we add columns that should be turned into <Keys>, should the frame not contain such columns jet.
*)

let exampleFrame = 
  frame [
    ("Column1")=>series ["row1"=>2.;"row2"=>4.5;"row3"=>3.7;"row4"=>2.9]
    ("Column2")=>series ["row1"=>4.;"row2"=>5.5;"row3"=>6.9;"row4"=>5.]
  ]

let exampleFrameWithKeyCols =
  exampleFrame
  |>Frame.addCol "Gen"(series ["row1"=>"A";"row2"=>"A";"row3"=>"A";"row4"=>"A"])
  |>Frame.addCol "TecRep"(series ["row1"=>"B";"row2"=>"B";"row3"=>"C";"row4"=>"C"])
  |>Frame.addCol "BioRep"(series ["row1"=>"D";"row2"=>"E";"row3"=>"D";"row4"=>"E"])

let indexedFrameWithKeyCols = indexWithColumnValues ["Gen";"TecRep";"BioRep"] exampleFrameWithKeyCols

indexedFrameWithKeyCols.Print()

(***include-output***) 

(**
as you can see, we have now a multi-tiered <Key> for each row that is unique. From this point, one can go in several directions.
For example, one could subtract or add to all values in a series (Column) with the `transform` function or one could create 
filters with the modules `GroupFilter` or `NumericFilter` or the `filter` function. Here we want to aggregate a single column. 
*)

let singleColumnAggregateMean = numAggregat Mean indexedFrameWithKeyCols "Column1" ["Gen";"TecRep"] (seq [])

singleColumnAggregateMean.Print()

(***include-output***)

