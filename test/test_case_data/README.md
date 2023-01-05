The files in this directory:

1M.rnd - randomly-generated file
1M_copy.rnd - a copy of the above randomly-generated file
1M_alt.rnd - a second, distinct 1M file

These files are used in the following directories

first_dir - has a copy of 1M and 1M_alt; these should not be considered duplicates.
second_dir - has a copy of 1M and 1M_copy; these should be considered duplicates.
A comparison between files should find first_dir/1M and second_dir/1M to be duplicates.
A comparison between files should find first_dir/1M and second_dir/1M_copy to be duplicates.
