Project 1 for Udacity Full Stack ND.

This program is designed to do some mathematical analysis of data from a database using ONLY SQL statements to do the bulk of the work.

How to run:

Install Vagrant and VirtualBox if it isn't installed.

`vagrant up` while in the project root and then ssh into the VM and go to `/app/`

Load the data into the database with `psql -d news -f newsdata.sql`

To execute the program, please run `python3 analysis.py`.
