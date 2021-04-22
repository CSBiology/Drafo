namespace Assemble

open Argu

module CLIArgumentParsing = 
    open System.IO
  
    type CLIArguments =
        | [<Mandatory>] [<AltCommandLine("-i")>]  InputFolder       of path:string 
        | [<Mandatory>] [<AltCommandLine("-kc")>] KeyColumns        of path:string list 
        | [<Mandatory>] [<AltCommandLine("-tc")>] TargetCol         of path:string 
        | [<Mandatory>] [<AltCommandLine("-o")>]  OutputDirectory   of path:string 

    with
        interface IArgParserTemplate with
            member s.Usage =
                match s with
                | InputFolder _      -> "specify path to input file"
                | KeyColumns _  -> "Specify the file path of the peptide data base."
                | TargetCol _             -> "specify GFF3 file"
                | OutputDirectory  _ -> "specify output directory"

