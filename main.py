import json
import unicodedata
import pprint
import street_types
import boto

# This script composes a person's record from two different csv data files.

ELECTORATE_FILE = 'aws_s3_western_vic_section_1.csv'
COMMERCIAL_RECORDS_FILE = 'aws_s3_pm_vic.csv'
MATCHED_RECORDS_FILE = 'matched_records_section_1.csv'


def parse_list(list):
    temp_list = []
    for entity in list:
        parsed_entity = [ x.strip().lower() for x in entity.split(',')]
        temp_list.append(parsed_entity)

    return temp_list


def strip_quotes(list):
    temp_list = []
    temp_element = ''

    for person in list:
        temp_person = []
        for element in person:
            temp_element = element[1: len(element) -1]
            temp_person.append(temp_element)
        temp_list.append(temp_person)

    return temp_list


def unabbreviate_address(list):
    temp_list = []

    # the sixth element of each person list contains the street address string
    for bung in list:
        long_address = bung[5]
        if long_address == '':
            # there is no street address for person
            # print 'in unabbreviate_address:: NO_GOOD: no street name for person'
            continue
        else:
            # find the start of the abbreviated street
            runner = len(long_address) - 1
            abbreviated_word = ''
            last_space = 0
            # print 'long address: %s' % long_address
            while runner >= 0: 
                # traverse the string from end to start
                # print long_address[runner:runner+1]

                if long_address[runner:runner+1].isspace():
                    last_space = int(runner) #index of last space
                    start = last_space + 1
                    end = len(long_address) - 1
                    abbreviated_word = long_address[start: end + 1]
                    # print 'abbreviated_word:%s' % abbreviated_word
                    break
                elif runner == 0:
                    # found no spaces, by dedault keep the long_address, so:
                    last_space = len(long_address) 

                runner -= 1 
            bung[5] = long_address[0: last_space + 1] + \
               street_types.verbose_version(abbreviated_word) 
            # print 'verbose version is now: %s' % bung[5]
            temp_list.append(bung)

    return temp_list 
   

def abbreviate_given_names(list):
    temp_list = []

    for person in list:
        # the given names string for the person is the second element
        given_names = person[1].strip(' ')
        # print given_names 

        # output formatting:
        # 'james earl' -> 'j e'
        # 'kerry-ann'  -> 'k a' 

        # get first initial, start of first word in string
        initials = given_names[0:1] 

        runner = 0
        while runner < len(given_names):
            current_char = given_names[runner: runner + 1]
            # print 'current_char: %s' % current_char
            if current_char  == ' ' or current_char == '-':
                initials = initials + ' ' + given_names[runner + 1: runner + 2]

            runner += 1
        # print '\'%s\'' % initials 

        # don't overwrite the persons verbose given names. that way you can address them
        # using their name, instead of just their initials. more personal approach.
        # insert abbreviation after the verbose given name 
        person.insert(2, initials)
        # person[1] = initials            
        temp_list.append(person)

    return temp_list


def make_comparisons(e_list, c_list):
    temp_list = []
    records_matched = 0
    
    for e_rec in e_list:
        for c_rec in c_list:

            e_sur      = e_rec[0]
            e_given    = e_rec[2]    
            e_address  = e_rec[4]             # e.g. '7 roncliffe road'
            e_postcode = e_rec[6]

            #print ('e_rec:', e_sur, e_given, e_address, e_postcode)

            c_sur      = c_rec[2]
            c_given    = c_rec[3]
            c_address  = c_rec[4] + ' ' + c_rec[5]  # '7' + ' ' +  'roncliffe road'
            c_postcode = c_rec[8]

            #print ('c_rec:', c_sur, c_given, c_address, c_postcode)

            if (e_sur == c_sur and 
               e_given == c_given and 
               e_address == c_address and 
               e_postcode == c_postcode):

                # should format out any () and spaces in phone number before appending it
                formatted_phone = format_phone_number(c_rec[9])

                e_rec.append(formatted_phone)
                temp_list.append(e_rec)
                #print '---------------FOUND MATCH-----------------'
                #pprint.pprint(e_rec)
                #print '^' * 60
                #pprint.pprint(c_rec)
              
                records_matched += 1
                print '\033[36m%d\033[39m' % records_matched

    return temp_list


def format_phone_number(phone_no):
    temp_phone_no = ''
    runner = 0 
    
    while runner < len(phone_no):
        #print 'yooo'
        cur_char = phone_no[runner: runner + 1]
        if cur_char == '(' or cur_char == ')' or cur_char == ' ': 
            #print 'yep'
            runner += 1            
            continue
        else:
            temp_phone_no = temp_phone_no + cur_char
            runner += 1


    #print 'old phone version: %s' % phone_no
    #print 'temp_phone_no: %s'% temp_phone_no
    return temp_phone_no


### Main section ###
if __name__ == '__main__':
    print 'Start your engines. Matching records and composing...'

    #USING AWS FOR FILE SERVING
    #make S3 connection using access keys from environment
    conn = boto.connect_s3()
    thewearyrat_bucket = conn.get_bucket('au.com.thewearyrat')
    print 'keys in au.com.thewearyrat bucket:'
    for vvv in thewearyrat_bucket.list():
        pprint.pprint(vvv)

    print 'getting WesternVic_section_1.csv from S3...'
    western_vic_s3 = thewearyrat_bucket.get_key('WesternVic_section_1.csv').get_contents_as_string()
    print 'getting PMVic.csv.csv from S3...'
    pm_vic_s3 = thewearyrat_bucket.get_key('PMVic.csv').get_contents_as_string()

    # create the files for data to go in
    try:
        western_vic_file_object_s3 =  open(ELECTORATE_FILE, 'w')
        pm_vic_file_object_s3 =       open(COMMERCIAL_RECORDS_FILE, 'w')
        western_vic_file_object_s3.writelines(western_vic_s3)
        pm_vic_file_object_s3.writelines(pm_vic_s3)
    finally:
        western_vic_file_object_s3.close()
        pm_vic_file_object_s3.close()
    

    # read the files for use. (i know that this is sub optimal)
    try:
        electorate_file_object =        open(ELECTORATE_FILE)
        com_records_file_object =       open(COMMERCIAL_RECORDS_FILE)
        matched_records_output_object = open(MATCHED_RECORDS_FILE, 'w')

        electorate_list = electorate_file_object.read().split('\n')
        com_records_list = com_records_file_object.read().split('\n')
        
        electorate_list.pop()
        com_records_list.pop()
    finally:
        electorate_file_object.close()
        com_records_file_object.close()


    # mould the two datasets into the a form suitable for comparing records
    electorate_list = abbreviate_given_names(parse_list(electorate_list))
    com_records_list = unabbreviate_address(strip_quotes(parse_list(com_records_list)))
    matched_records = make_comparisons(electorate_list, com_records_list)

    # Write output results to files
    for person in matched_records:
        da_person = [',' + x for x in person ]
        da_person = ''.join(da_person) + '\n'
        #print 'da_person: %s' % da_person[1:]
        matched_records_output_object.writelines(da_person[1:])

    # Close files after writing
    matched_records_output_object.close()



    # now upload the matched_records to S3
    print 'Uploading ' + MATCHED_RECORDS_FILE + 'to S3...'
    from boto.s3.key import Key
    k = Key(thewearyrat_bucket) 
    k.key = MATCHED_RECORDS_FILE

    try:
        matched_records_output_object = open(MATCHED_RECORDS_FILE)
        matched_records = matched_records_output_object.read()
    finally:
        matched_records_output_object.close()
    
    k.set_contents_from_string(matched_records) 

    #pprint.pprint(electorate_list)
    # print '*' * 60
    print '!' * 60
    print 'bueno'
    #pprint.pprint(matched_records)
