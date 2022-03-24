
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
open NumericFilter.NumericFilter
open NumericAggregation.NumericAggregation

(**
## Example workflow

If one has a csv that contains the data the first question is whether the frame contains columns that are needed for the indexing.
Should this be the case one can use the `readAndIndexFrame`, is that not the case one has first to readin the frame with 
`readFrame` add the columns that are neaded for indexing and then index with `indexWithColumnValues`. 
You will need to index with at least 2 columns should you wish to `aggregate` because then can have the same Key 
value paires in one column as needed/wished for the aggregation. Each time you want to aggregate means you have to have an additional column
for the indexing. Keep in mind that you can use funtions of the `GroupTransform` module as often as you want because the row keys are not changed.In this example we go with 2 aggregations because we have biological and technical replicates. 

*)
let frameForTutorial = 
    frame [
        ("ConditionA")=> series ["row1" => 2.;"row2" =>3.;"row3" =>1.;"row4" =>7.]
        ("ConditionB")=> series ["row1" => 2.4;"row2" =>4.5;"row3" =>6.1;"row4" =>5.1]    
    ]
    |>Frame.addCol "Gen"(series ["row1" => "A" ;"row2" =>"A";"row3" =>"A";"row4" =>"A"])
    |>Frame.addCol "technicalReplicate"(series ["row1" => "B" ;"row2" =>"B";"row3" =>"C";"row4" =>"C"])
    |>Frame.addCol "BioRep"(series ["row1" => "D" ;"row2" =>"E";"row3" =>"D";"row4" =>"E"])

let indexedTutorialFrame = indexWithColumnValues ["Gen";"technicalReplicate";"BioRep"] frameForTutorial
indexedTutorialFrame.Print()

(***include-output***)  

(**
!!!! 
Keep in mind that the function working on the whole frame will error should 
you have strings as values inside the frame for the `NumericAggregation` function or ints/floats for the `StringAggregation`.
So remove the values/columns or use the single column versions multiple times.
!!!!
*)
let indexedFrameWithoutIndexingColumns = 
    indexedTutorialFrame
    |>Frame.dropCol "Gen"
    |>Frame.dropCol "technicalReplicate"
    |>Frame.dropCol "BioRep"


indexedFrameWithoutIndexingColumns.Print()
(***include-output***)  
(**
As you can see each row has multiple Key objects and the combinations are unique.
Now one can create a filter with either the `NumericFilter` or `GroupFilter` module functions.
to use the resultant series<Key,bool> or seq series<Key,bool> one has to keep in mind that the functions in the 
`NumericAggregation` and `StringAggregation` modules need for each column a seq of series<Key,bool>, so one needs 
to create a seq of seq of series<Key,bool> for the function that affect the whole frame. If not done correctly this could
result in an error(an empty filter is not suitable for the functions working on the whole frame!).
When you have a suitable seq of series<Key,bool> or seq of seq of series<Key,bool> then one can then use 
the aggregation module of choice. 
Let's look back at our example with filters that filters would always say true, we want the Mean and we use the seq ["Gen";"technicalReplicate"] to determine that Rep. 

*)

let filterA = numericFilterServeralCol (IsBiggerThan 0.5) indexedFrameWithoutIndexingColumns
let filterB = numericFilterServeralCol (IsSmallerThan 100.) indexedFrameWithoutIndexingColumns

let seqOfFilterMulty filterOne filterTwo =  
    filterOne
    |>Seq.mapi (fun i x -> 
        let colKeysForSeq seqIt b= 
            seqIt
            |>Seq.item b
        let cIII = seq [x;colKeysForSeq filterTwo i]
        cIII)

let aggregatedFrameA = numAgAllCol Mean indexedFrameWithoutIndexingColumns ["Gen";"technicalReplicate"] (seqOfFilterMulty filterA filterB)
aggregatedFrameA.Print()
(***include-output***)  
(**
As you can see the `BioRep` parts of the keys was dropped and we aggregated Keys that were identical. 
Now we also want to do the median of the `technicalReplicate`. For that we need new filters and need to adjust the input parameters.

*)

let filterC = numericFilterServeralCol (IsBiggerThan 0.5) aggregatedFrameA 
let filterD = numericFilterServeralCol (IsSmallerThan 100.) aggregatedFrameA 

let aggregatedFrameB = numAgAllCol Median aggregatedFrameA  ["Gen"] (seqOfFilterMulty filterC filterD)
aggregatedFrameB.Print()
(***include-output***)







