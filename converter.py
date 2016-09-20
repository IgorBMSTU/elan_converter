# -*- coding: utf-8 -*-
import sqlite3
import base64
import requests
import json
import hashlib
import logging

import sqlite3
import base64
import requests
import json
import hashlib
import logging
import conv

import base64
import hashlib
from pydub import AudioSegment
from collections import defaultdict

SOUND_FILE_PATH = "w1.wav"
EAF_FILE_PATH = "/home/igor/ELAN_4.9.4/katushka2.eaf"
SOUND_FOLDER = "sound"

def convert_db_new(sqconn, session, client_id, server_url, locale_id=1):
    link_dict = defaultdict(list)
    dubl = []
    first_perspective_fields = defaultdict(dict)
    second_perspective_fields = defaultdict(dict)
    log = logging.getLogger(__name__)
    converter = conv.Elan(EAF_FILE_PATH)
    converter.parse()
    final_dicts = converter.proc()

    """

    DICTIONARY CREATION

    """



    status = session.get(server_url + 'all_data_types')
    field = json.loads(status.text)

    all_data_types = { x["contains"][0]["content"]: (x["client_id"], x["object_id"]) for x in field }
    link_dt_client_id, link_dt_object_id = all_data_types["Link"]
    sound_dt_client_id, sound_dt_object_id = all_data_types["Sound"]
    text_dt_client_id, text_dt_object_id = all_data_types["Text"]
    print(link_dt_client_id, link_dt_object_id)



    print(all_data_types)
    #return
    #create_languages_request = {"object_id": 11,
    #                            "client_id": client_id
    #                            }
    #status = session.get(server_url + 'languages', data=json.dumps(create_languages_request))
    #language = json.loads(status.text)
    #   print(status)

    create_gist_request = {"type": "Dictionary" }
    status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
    gist = json.loads(status.text)

    gist_client_id = gist["client_id"]
    gist_object_id = gist["object_id"]
    create_atom_request = {"parent_client_id": gist_client_id,
                           "parent_object_id": gist_object_id,
                           "locale_id":2,
                           "content":"dictionary"
                          }

    status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
    atom = json.loads(status.text)
    atom_client_id = atom["client_id"]
    atom_object_id = atom["object_id"]
    create_dictionary_request =     {
    "translation_gist_client_id": gist_client_id,
    "translation_gist_object_id": gist_object_id,
    "parent_client_id": client_id,
    "parent_object_id": 11,
    }
    status = session.post(server_url + 'dictionary', data=json.dumps(create_dictionary_request))
    dictionary = json.loads(status.text)
    dictionary_client_id = dictionary["client_id"]
    dictionary_object_id = dictionary["object_id"]

    """

    FIRST PERSPECTIVE CREATION

    """

    create_perspective_request =     {
    "translation_gist_client_id": gist_client_id,
    "translation_gist_object_id": gist_object_id,
    "parent_client_id": client_id,
    "parent_object_id": 11,
    }
    status = session.post(server_url + 'dictionary/%s/%s/perspective' % (dictionary_client_id,
                                                                         dictionary_object_id),
                                                                         data=json.dumps(create_perspective_request)
                          )
    first_perspective = json.loads(status.text)
    first_perspective_client_id = first_perspective["client_id"]
    first_perspective_object_id = first_perspective["object_id"]
    print(first_perspective)


    """

    SECOND PERSPECTIVE CREATION

    """

    create_perspective_request =     {
    "translation_gist_client_id": gist_client_id,
    "translation_gist_object_id": gist_object_id,
    "parent_client_id": client_id,
    "parent_object_id": 11,
    }
    status = session.post(server_url + 'dictionary/%s/%s/perspective' % (dictionary_client_id,
                                                                         dictionary_object_id),
                                                                         data=json.dumps(create_perspective_request))
    second_perspective = json.loads(status.text)
    second_perspective_client_id = second_perspective["client_id"]
    second_perspective_object_id = second_perspective["object_id"]
    print(second_perspective)

    """

    FIRST PERSPECTIVE FIELDS CREATION

    """

    # tier names creation

    update_fields_request = []
    print(converter.tiers)
    for tier in converter.tiers:
        if tier in converter.noref_tiers:
            create_gist_request = {"type": "Dictionary" }
            status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
            gist = json.loads(status.text)
            ##print(gist)
            gist_client_id = gist["client_id"]
            gist_object_id = gist["object_id"]
            create_atom_request = {"parent_client_id": gist_client_id,
                                   "parent_object_id": gist_object_id,
                                   "locale_id":2,
                                   "content": tier
                                  }
            status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
            atom = json.loads(status.text)
            ##print(atom)
            atom_client_id = atom["client_id"]
            atom_object_id = atom["object_id"]
            create_field_request =         {
                "translation_gist_client_id": gist_client_id,
                "translation_gist_object_id": gist_object_id,
                "data_type_translation_gist_client_id": text_dt_client_id,
                "data_type_translation_gist_object_id": text_dt_object_id
                }
            status = session.post(server_url + 'field', data=json.dumps(create_field_request))
            field = json.loads(status.text)
            field_client_id = int(field["client_id"])
            field_object_id = int(field["object_id"])
            update_fields_request.append(
                {
                "client_id":field_client_id,
                "object_id":field_object_id
                }
                )
            first_perspective_fields[tier] = (field_client_id, field_object_id)
    # link field creation

    create_gist_request = {"type": "Dictionary" }
    status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
    gist = json.loads(status.text)
    ##print(gist)
    gist_client_id = gist["client_id"]
    gist_object_id = gist["object_id"]
    create_atom_request = {"parent_client_id": gist_client_id,
                           "parent_object_id": gist_object_id,
                           "locale_id":2,
                           "content":"Link"
                          }
    status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
    atom = json.loads(status.text)
    ##print(atom)
    atom_client_id = atom["client_id"]
    atom_object_id = atom["object_id"]
    create_field_request =         {
        "translation_gist_client_id":gist_client_id,
        "translation_gist_object_id":gist_object_id,
        "data_type_translation_gist_client_id":link_dt_client_id,
        "data_type_translation_gist_object_id":link_dt_object_id
        }
    status = session.post(server_url + 'field', data=json.dumps(create_field_request))
    field = json.loads(status.text)
    fp_link_client_id = int(field["client_id"])
    fp_link_object_id = int(field["object_id"])
    update_fields_request.append(
        {
        "client_id":fp_link_client_id,
        "object_id":fp_link_object_id,
        "link":{
            "client_id": second_perspective_client_id,
            "object_id": second_perspective_object_id
        }
        }
        )


    create_gist_request = {"type": "Dictionary" }
    status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
    gist = json.loads(status.text)
    ##print(gist)
    gist_client_id = gist["client_id"]
    gist_object_id = gist["object_id"]
    create_atom_request = {"parent_client_id": gist_client_id,
                           "parent_object_id": gist_object_id,
                           "locale_id":2,
                           "content":"SOUND"
                          }
    status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
    atom = json.loads(status.text)
    ##print(atom)
    atom_client_id = atom["client_id"]
    atom_object_id = atom["object_id"]
    create_field_request =         {
        "translation_gist_client_id": gist_client_id,
        "translation_gist_object_id": gist_object_id,
        "data_type_translation_gist_client_id": sound_dt_client_id,
        "data_type_translation_gist_object_id": sound_dt_object_id
        }
    status = session.post(server_url + 'field', data=json.dumps(create_field_request))
    field = json.loads(status.text)
    fp_sound_field_client_id = int(field["client_id"])
    fp_sound_field_object_id = int(field["object_id"])
    update_fields_request.append(
        {
        "client_id":fp_sound_field_client_id,
        "object_id":fp_sound_field_object_id,
        }
        )



    print("==")

    #print(status.text)
    print(client_id)

    status = session.put(server_url + 'dictionary/%s/%s/perspective/%s/%s/fields' % (dictionary_client_id,
                                                                                     dictionary_object_id,
                                                                                     first_perspective_client_id,
                                                                                     first_perspective_object_id),
                                                                                     data=json.dumps(update_fields_request))
    field = json.loads(status.text)
    print(status)
    print(field)
    #return


    """

    SECOND PERSPECTIVE FIELDS CREATION

    """
    update_fields_request = []

    ###print(converter.tiers)
    ###print(len(converter.tiers))
    for tier in converter.tiers:
        if tier not in converter.noref_tiers or tier == conv.MAIN_TIER:
            create_gist_request = {"type": "Dictionary" }
            status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
            gist = json.loads(status.text)
            ##print(gist)
            gist_client_id = gist["client_id"]
            gist_object_id = gist["object_id"]
            create_atom_request = {"parent_client_id": gist_client_id,
                                   "parent_object_id": gist_object_id,
                                   "locale_id":2,
                                   "content":tier
                                  }
            status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
            atom = json.loads(status.text)
            ##print(atom)
            atom_client_id = atom["client_id"]
            atom_object_id = atom["object_id"]
            create_field_request =         {
                "translation_gist_client_id": gist_client_id,
                "translation_gist_object_id": gist_object_id,
                "data_type_translation_gist_client_id": text_dt_client_id,
                "data_type_translation_gist_object_id": text_dt_object_id
                }
            status = session.post(server_url + 'field', data=json.dumps(create_field_request))
            field = json.loads(status.text)
            field_client_id = int(field["client_id"])
            field_object_id = int(field["object_id"])
            update_fields_request.append(
                {
                "client_id":field_client_id,
                "object_id":field_object_id
                }
                )
            second_perspective_fields[tier] = (field_client_id, field_object_id)



    # link field creation

    create_gist_request = {"type": "Dictionary" }
    status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
    gist = json.loads(status.text)
    ##print(gist)
    gist_client_id = gist["client_id"]
    gist_object_id = gist["object_id"]
    create_atom_request = {"parent_client_id": gist_client_id,
                           "parent_object_id": gist_object_id,
                           "locale_id":2,
                           "content":"Link"
                          }
    status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
    atom = json.loads(status.text)
    ##print(atom)
    atom_client_id = atom["client_id"]
    atom_object_id = atom["object_id"]
    create_field_request =         {
        "translation_gist_client_id": gist_client_id,
        "translation_gist_object_id": gist_object_id,
        "data_type_translation_gist_client_id": link_dt_client_id,
        "data_type_translation_gist_object_id": link_dt_object_id
        }
    status = session.post(server_url + 'field', data=json.dumps(create_field_request))
    field = json.loads(status.text)
    sp_link_client_id = int(field["client_id"])
    sp_link_object_id = int(field["object_id"])
    update_fields_request.append(
        {
        "client_id": sp_link_client_id,
        "object_id": sp_link_object_id,
        "link":{
            "client_id": first_perspective_client_id,
            "object_id": first_perspective_object_id
        }
        }
        )



    ##
    ##print(update_fields_request)

    # sound field creation
    create_gist_request = {"type": "Dictionary" }
    status = session.post(server_url + 'translationgist', data=json.dumps(create_gist_request))
    gist = json.loads(status.text)
    ##print(gist)
    gist_client_id = gist["client_id"]
    gist_object_id = gist["object_id"]
    create_atom_request = {"parent_client_id": gist_client_id,
                           "parent_object_id": gist_object_id,
                           "locale_id":2,
                           "content":"SOUND"
                          }
    status = session.post(server_url + 'translationatom', data=json.dumps(create_atom_request))
    atom = json.loads(status.text)
    ##print(atom)
    atom_client_id = atom["client_id"]
    atom_object_id = atom["object_id"]
    create_field_request =         {
        "translation_gist_client_id": gist_client_id,
        "translation_gist_object_id": gist_object_id,
        "data_type_translation_gist_client_id": sound_dt_client_id,
        "data_type_translation_gist_object_id": sound_dt_object_id
        }
    status = session.post(server_url + 'field', data=json.dumps(create_field_request))
    print(status)
    field = json.loads(status.text)
    sp_sound_field_client_id = int(field["client_id"])
    sp_sound_field_object_id = int(field["object_id"])
    update_fields_request.append(
        {
        "client_id": sp_sound_field_client_id,
        "object_id": sp_sound_field_object_id
        }
        )
    print(update_fields_request)

    ##(update_fields_request)
    status = session.put(server_url + 'dictionary/%s/%s/perspective/%s/%s/fields' % (dictionary_client_id,
                                                                                     dictionary_object_id,
                                                                                     second_perspective_client_id,
                                                                                     second_perspective_object_id),
                                                                                     data=json.dumps(update_fields_request))
    print(status)

    field = json.loads(status.text)
    print(field)

    #field_client_id = field["client_id"]
    #field_object_id = field["object_id"]
    print("!/!", field)
    for phrase in final_dicts:
        ##print(phrase)
        # 1 lexical_entry creation
        status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry' % (dictionary_client_id, dictionary_object_id, first_perspective_client_id, first_perspective_object_id)  ) # data=json.dumps(create_lexical_entry_request))
        lexical_entry = json.loads(status.text)
        fp_lexical_entry_client_id = lexical_entry["client_id"]
        fp_lexical_entry_object_id = lexical_entry["object_id"]
        curr_dict = None
        for word_translation in phrase:
            if type(word_translation) is not list:
                    #print("!-> ", word_translation, type(word_translation) )
                    curr_dict = word_translation
                    main_tier_text = " ".join([i.text for i in word_translation])
                    print(main_tier_text)
                    entity_request = {"locale_id": 2,
                                         "data_type": "text",
                                         "field_client_id": first_perspective_fields[conv.MAIN_TIER][0],
                                         "field_object_id" : first_perspective_fields[conv.MAIN_TIER][1],
                                         "content": main_tier_text}
                    status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                          dictionary_object_id,
                                                                                                                          first_perspective_client_id,
                                                                                                                          first_perspective_object_id,
                                                                                                                          fp_lexical_entry_client_id,
                                                                                                                          fp_lexical_entry_object_id) , data=json.dumps(entity_request) )
            else:
                word = word_translation[0]
                print(word._print())
                tier_name = word.tier
                new = " ".join([i.text for i in word_translation])
                print(tier_name, "|", new , len(new))
                entity_request = {"locale_id": 2,
                                     "data_type": "text",
                                     "field_client_id": first_perspective_fields[tier_name][0],
                                     "field_object_id" : first_perspective_fields[tier_name][1],
                                     "content": new}
                status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                      dictionary_object_id,
                                                                                                                      first_perspective_client_id,
                                                                                                                      first_perspective_object_id,
                                                                                                                      fp_lexical_entry_client_id,
                                                                                                                      fp_lexical_entry_object_id) , data=json.dumps(entity_request) )
                if word.tier == converter.tiers[0]:
                    with open("%s/%s.wav" %(SOUND_FOLDER, word.index), "rb") as f:
                        audio_slice = f.read()
                        audio_hash = hashlib.sha224(audio_slice).hexdigest()
                        #print(str(word.index)+".wav")
                        entity_request = {"locale_id": 2,
                                             "filename": "%s/%s.wav" % (SOUND_FOLDER, word.index),
                                             "field_client_id": fp_sound_field_client_id,
                                             "field_object_id": fp_sound_field_object_id,
                                             "content": base64.urlsafe_b64encode(audio_slice).decode()
                                          }
                        status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                              dictionary_object_id,
                                                                                                                              second_perspective_client_id,
                                                                                                                              second_perspective_object_id,
                                                                                                                              fp_lexical_entry_client_id,
                                                                                                                              fp_lexical_entry_object_id) , data=json.dumps(entity_request) )
                        print(status)


        for word in curr_dict:
            column = [word] + curr_dict[word]
            cort = tuple(i.text for i in column)



            if cort in link_dict:
                sp_lexical_entry_client_id, sp_lexical_entry_object_id = link_dict[cort]
                print("[][][]", cort)



            else:
                print("()()()", cort)
                #print(word.text)

                # lexical entry
                status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry' % (dictionary_client_id, dictionary_object_id, second_perspective_client_id, second_perspective_object_id)  ) # data=json.dumps(create_lexical_entry_request))
                lexical_entry = json.loads(status.text)
                sp_lexical_entry_client_id = int(lexical_entry["client_id"])
                sp_lexical_entry_object_id = int(lexical_entry["object_id"])
                link_dict[cort] = (sp_lexical_entry_client_id, sp_lexical_entry_object_id)
                entity_request = {"locale_id": 2,
                                     "data_type": "text",
                                     "field_client_id": second_perspective_fields[word.tier][0],
                                     "field_object_id": second_perspective_fields[word.tier][1],
                                     "content": word.text}
                status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                      dictionary_object_id,
                                                                                                                      second_perspective_client_id,
                                                                                                                      second_perspective_object_id,
                                                                                                                      sp_lexical_entry_client_id,
                                                                                                                      sp_lexical_entry_object_id) , data=json.dumps(entity_request) )
                for other_word in curr_dict[word]:
                    entity_request = {"locale_id": 2,
                                         "data_type": "text",
                                         "field_client_id": second_perspective_fields[other_word.tier][0],
                                         "field_object_id": second_perspective_fields[other_word.tier][1],
                                         "content": other_word.text}
                    status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                          dictionary_object_id,
                                                                                                                          second_perspective_client_id,
                                                                                                                          second_perspective_object_id,
                                                                                                                          sp_lexical_entry_client_id,
                                                                                                                          sp_lexical_entry_object_id) , data=json.dumps(entity_request) )
                    #print(other_word.text)

                #print(status)
                #"""

                with open("%s/%s.wav" % (SOUND_FOLDER, word.index), "rb") as f:
                    audio_slice = f.read()
                    audio_hash = hashlib.sha224(audio_slice).hexdigest()
                    #print(str(word.index)+".wav")
                    entity_request = {"locale_id": 2,
                                         "filename": str(word.index)+".wav",
                                         "field_client_id": sp_sound_field_client_id,
                                         "field_object_id": sp_sound_field_object_id,
                                         "content": base64.urlsafe_b64encode(audio_slice).decode()
                                      }
                    status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                                          dictionary_object_id,
                                                                                                                          second_perspective_client_id,
                                                                                                                          second_perspective_object_id,
                                                                                                                          sp_lexical_entry_client_id,
                                                                                                                          sp_lexical_entry_object_id) , data=json.dumps(entity_request) )
                    #print(status)
                    #print(audio_slice)
                    #print(status)
                #"""
            dubl_tuple = ((fp_lexical_entry_client_id, fp_lexical_entry_object_id), (sp_lexical_entry_client_id, sp_lexical_entry_object_id))
            if not  dubl_tuple in dubl:
                dubl.append(dubl_tuple)
                entity_request = {"locale_id": 2,
                                     "data_type": "link",
                                     "field_client_id": fp_link_client_id,
                                     "field_object_id": fp_link_object_id,
                                     "parent_client_id": fp_lexical_entry_client_id,
                                     "parent_object_id": fp_lexical_entry_object_id,
                                     "link_client_id": sp_lexical_entry_client_id,
                                     "link_object_id": sp_lexical_entry_object_id
                                  }



                status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                              dictionary_object_id,
                                                                                                              first_perspective_client_id,
                                                                                                              first_perspective_object_id,
                                                                                                              fp_lexical_entry_client_id,
                                                                                                              fp_lexical_entry_object_id) , data=json.dumps(entity_request) )


                entity_request = {"locale_id": 2,
                                     "data_type": "link",
                                     "field_client_id": sp_link_client_id,
                                     "field_object_id": sp_link_object_id,
                                     "parent_client_id": sp_lexical_entry_client_id,
                                     "parent_object_id": sp_lexical_entry_object_id,
                                     "link_client_id": fp_lexical_entry_client_id,
                                     "link_object_id": fp_lexical_entry_object_id
                                  }



                status = session.post(server_url + 'dictionary/%s/%s/perspective/%s/%s/lexical_entry/%s/%s/entity' % (dictionary_client_id,
                                                                                                              dictionary_object_id,
                                                                                                              second_perspective_client_id,
                                                                                                              second_perspective_object_id,
                                                                                                              sp_lexical_entry_client_id,
                                                                                                              sp_lexical_entry_object_id) , data=json.dumps(entity_request) )








        print("===")
    print(link_dict)
    #print(client_id, 11)
    return






def convert_one(filename, login, password,
                server_url="http://localhost:6543/"):
    print(1)
    log = logging.getLogger(__name__)
    log.debug("Starting convert_one")
    log.debug("Creating session")
    session = requests.Session()
    session.headers.update({'Connection': 'Keep-Alive'})
    log.debug("Going to login")
    login_data = {"login": login,
                  "password": password}
    log.debug("Login data: " + login_data['login'] + login_data['password'])
    cookie_set = session.post(server_url + 'signin', data=json.dumps(login_data))
    response = session.cookies.get_dict()
    client_id = response["client_id"]
    #object_id = response["object_id"]
    print(response)




    log.debug("Login status:" + str(cookie_set.status_code))
    if cookie_set.status_code != 200:
        log.error("Cheat login for conversion was unsuccessful")
        exit(-1)
    sqconn = sqlite3.connect(filename)
    log.debug("Connected to sqlite3 database")
    try:
        status = convert_db_new(sqconn, session, client_id, server_url)
    except Exception as e:
        log.error("Converting failed")
        log.error(e.__traceback__)
        raise
    log.debug(status)
    return status


def save_audio(full_audio):
    converter = conv.Elan(EAF_FILE_PATH)
    converter.parse()
    final_dicts = converter.proc()
    #print(converter.get_annotation_data_for_tier("text"))
    for phrase in final_dicts:
        for word_translation in phrase:
            if type(word_translation) is not list:
                for word in word_translation:
                    #print(word.text)
                    full_audio[ word.time[0]: word.time[1]].export("%s/%s.wav" % (SOUND_FOLDER, word.index), format="wav")
            else:
                #dublicate
                for word in word_translation:
                    if word.tier == converter.tiers[0]:
                        full_audio[ word.time[0]: word.time[1]].export("%s/%s.wav" % (SOUND_FOLDER, word.index), format="wav")

        #print("===")
if __name__ == "__main__":
    full_audio = AudioSegment.from_wav(SOUND_FILE_PATH)
    save_audio(full_audio)
    #import sys
    #sys.exit(0)
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
    logging.basicConfig(format='%(asctime)s\t%(levelname)s\t[%(name)s]\t%(message)s')
    convert_one(filename="/home/igor/db7.sqlite", login="Test",
                password="123456",
                server_url="http://localhost:6543/")