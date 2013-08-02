"""
#################################################
The ID list for subscriptions:
#################################################

**0-9 : PC & Time Data**
0: clientname			-- str (atlas)
1: timestamp (PC)		-- str (2013-01-23 12:10:32.712475)
2: date (PC)			-- str (2013-01-23)
3: outtime (PC)	 		-- str (12:10:32.712475)
4: timestamp (sensor)		-- str (2013-01-23 12:10:32.712475)
5: GPS coordinates		-- str (??.??N ??.??E)
    6-9:			-- [reserved]

**10-29 : Magnetic Data**
10: f				-- float (48633.04) [nT]
11: x				-- float (20401.3) [nT]
12: y				-- float (-30.0) [nT]
13: z				-- float (43229.7) [nT]
14: df				-- float (0.06) [nT]
    15-29: 			-- [reserved]

**30-39 : Temperature and Environment Data**
30: T (ambient)			-- float (7.2) [C]
31: T (sensor)			-- float (10.0) [C]
32: T (electronics)		-- float (12.5) [C]
33: humidity			-- float (99.0) [%]
34: T (dewpoint)		-- float (6.0) [C]
   35-39: 			-- [reserved]

**40+ : Other Variables**
40: Error code (POS1)		-- float (80) [-]
   41-98: 			-- [reserved]

99: eol				-- str (eol)
"""
