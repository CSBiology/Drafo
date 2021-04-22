#r "nuget: Deedle, 2.3.0"
open Deedle

let testFrame = 
      let index1 = 
        Series.ofValues [
          "one"
          "two"
          "three"
          "four"    
        ]
      let index2 = 
        Series.ofValues [
          1
          2
          3
          4    
        ]
      let index3 = 
        Series.ofValues [
          "group 1"
          "group 1"
          "group 2"
          "group 2"    
        ]
      let value1 = 
        Series.ofValues [
          1000.
          2000.
          nan
          1.    
        ]
      let value2 = 
        Series.ofValues [
          10000.
          20000.
          nan
          10.    
        ]  
      Frame.ofColumns(["index1",index1])
      |> Frame.addCol "index2" index2
      |> Frame.addCol "index3" index3
      |> Frame.addCol "value1" value1
      |> Frame.addCol "value2" value2

let p = __SOURCE_DIRECTORY__
testFrame.SaveCsv(System.IO.Path.Combine(p,"testFrame.txt"),includeRowKeys=false,separator='\t')      