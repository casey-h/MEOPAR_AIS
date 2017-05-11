#!/usr/bin/env python

"""
First, find a message exemplar (none in 2015-01 !!)
ee_ais=> select * from ais_s_201501 where message_id = 8 and ais_msg_eecsv like '%|1|24|%' limit 1;
--0 Records

select * from [ais_s_* / ee_ais_master] where message_id = 8 and ais_msg_eecsv like '%|1|24|%' limit 1;

"""
import binascii


def unpack_msg_8(dac, fl, strInMsg):
    if dac = 1:
        if fl = 24:
            bitstring = bin(int(binascii.hexlify(strInMsg), 16))
            
        else:
            return "Unrecognized message spec.\n"
    else:
        return "Unrecognized message spec.\n"


def main():
    res = unpack_msg_8(
    
