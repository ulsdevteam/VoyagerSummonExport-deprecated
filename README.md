VoyagerSummonExport
===================

Perl module to export MARC records from [Exlibris Voyager] (http://www.exlibrisgroup.com/category/Voyager) to [ProQuest Summon] (http://www.serialssolutions.com/en/services/summon/) for indexing.  Handles the initial full export, and daily updates and deletes.  Assumes a *nix environment and requires direct access to the Voyager database, Pmarcexport executable, and Voyager "rpt" log files.

See the following ProQuest Summon KB articles:
* [Exporting Catalog Holdings - Uploading to Summon] (https://proquestsupport.force.com/portal/homePage?id=kA0400000004J6wCAE)
* [Catalog File Naming Conventions] (https://proquestsupport.force.com/portal/homePage?id=kA0400000004J6wCAE)
* [How to delete catalog records from the Summon Unified Index] (https://proquestsupport.force.com/portal/homePage?id=kA0400000004J7jCAE)

Loosely based on a presentation by John Greer of University of Montana at ELUNA 2010: ["Voyager to Summon"] (http://documents.el-una.org/506/)

Other solutions may be found at [Michael Doran's Voyager MARC record export script] (http://rocky.uta.edu/doran/voyager/export/)

Created for the University of Texas at San Antonio / maintained for the University of Pittsburgh by Clinton Graham <ctgraham@pitt.edu> 412-383-1057.
