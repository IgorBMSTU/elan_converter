# -*- coding: utf-8 -*-
import pympi

import collections
try: from xml.etree import cElementTree as ElementTree
except ImportError: from xml.etree import ElementTree

MAIN_TIER = "IPA"

class Word:
    def __init__(self, index=None, text=None, tier=None, time=None):
        self.index = index
        self.text = text
        self.tier = tier
        self.time = time

    def _print(self):
        return((self.index, self.text, self.tier, self.time))

class Elan:
    def __init__(self, eaf_path):
        self.result = collections.OrderedDict()
        self.word_tier = {}
        self.tiers = []
        #self.tier_names = {}
        self.tier_refs = collections.defaultdict(list)
        self.word = {}
        self.main_tier = []
        self.noref_tiers = []
        self.reader = ElementTree.parse(eaf_path)
        self.xml_obj = self.reader.getroot()
        self.eafob = pympi.Elan.Eaf(eaf_path)


    def get_annotation_data_for_tier(self, id_tier):
        a = self.eafob.tiers[id_tier][0]
        return [(self.eafob.timeslots[a[b][0]], self.eafob.timeslots[a[b][1]], a[b][2])
                for b in a]

    def get_annotation_data_between_times(self, id_tier, start, end):
        tier_data = self.eafob.tiers[id_tier][0]
        anns = ((self.eafob.timeslots[tier_data[a][0]], self.eafob.timeslots[tier_data[a][1]], a)
                for a in tier_data)
        sorted_words = sorted([a for a in anns if a[0] >= start and a[1] <= end],  key=lambda t: t[1] )
        #print(sorted_words[0])
        return [x for x in sorted_words]
        #sorted([a[2] for a in anns if a[1] >= start and a[0] <= end],  key=lambda t: t[0] )


    def parse(self):
        for element in self.xml_obj:
            if element.tag == 'TIER':
                tier_id = element.attrib['TIER_ID']
                if 'PARENT_REF' in element.attrib:
                    tier_ref = element.attrib['PARENT_REF']
                    self.tier_refs[tier_ref].append(tier_id)
                self.tiers.append(tier_id)
                for elem1 in element:
                    if elem1.tag == 'ANNOTATION':
                        for elem2 in elem1:
                            self.word_tier[elem2.attrib["ANNOTATION_ID"]] = tier_id
                            if tier_id == MAIN_TIER:
                                self.main_tier.append(elem2.attrib["ANNOTATION_ID"])
                            self.word[elem2.attrib["ANNOTATION_ID"]] = [x for x in elem2][0].text
                            if elem2.tag == 'REF_ANNOTATION':
                                annot_ref = elem2.attrib['ANNOTATION_REF']

                                if not annot_ref in self.result:
                                    self.result[annot_ref] = []
                                self.result[annot_ref].append(elem2.attrib["ANNOTATION_ID"])

    def get_word_text(self, word):
        return list(word)[0].text

    def get_word_aid(self, word):
        return word.attrib['ANNOTATION_ID']

    def proc(self):
        for tier in self.tiers:
            tier_data = self.get_annotation_data_for_tier(tier)
            if tier_data:
                self.noref_tiers.append(tier)
        res = {}
        for aid in self.main_tier:
            res[aid] = []
            next_id = aid
            while next_id in self.result:
                next_id = self.result[next_id][0]
                res[aid].append(next_id)
        perspectives = []
        ans = sorted(self.eafob.get_annotation_data_for_tier(self.noref_tiers[0]), key=lambda t: t[0]) # text
        prev_flag = None
        for x in ans:
            next = []
            for cur_tier in self.noref_tiers:
                perspectives2 = collections.OrderedDict()

                if cur_tier == MAIN_TIER:
                    for k in self.get_annotation_data_between_times(cur_tier, x[0], x[1]):
                        t = (k[0], k[1])
                        z = k[2]
                        new_list = [Word(i, self.word[i], self.word_tier[i], (t[0], t[1])) for i in res[z]]
                        if new_list:
                            perspectives2[Word(z, self.word[z], cur_tier, (t[0], t[1]))] = new_list
                    next.append(perspectives2)
                else:
                    next.append([Word(i[2] , self.word[i[2]], cur_tier, (i[0], i[1])) for i in self.get_annotation_data_between_times(cur_tier, x[0], x[1])])

            perspectives.append(next)
            ###print("==")
        return perspectives


def main():
    pass
    #converter = Elan("/home/igor/ELAN_4.9.4/katushka2.eaf")
    #converter.parse()
    #final_dicts = converter.proc()



if __name__ == "__main__":
    main()

print( MAIN_TIER)