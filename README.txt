Note: this code has been completely refactored and included in the 
XENON1T github here: https://github.com/XENON1T/jax
The version shown here is completely different and was an early alpha
version.

Online data monitor backend script for XENON1T
D.Coderre 30.8.2016

# What's it do?

Processes some events and puts a reduced output into a mongodb for
online monitoring aggregation. It does this based on our file format's
structure where we get zips of 1000 events each. You set the variables
SF_MIN and SF_MAX to define the minimum and maximum number of events
to process out of each of these files. The option N_PROC tells hax
how many events to process at once.

# Running

Install pax, hax as per instructions. Hax requires some X libraries
(it will complain if you don't have them). 

Set database passwords and usernames via environment variables.

python monitor.py

# Complaints?

No warranty
