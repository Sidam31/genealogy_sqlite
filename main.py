#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from distutils.log import warn
from genericpath import exists
from bs4 import BeautifulSoup

import requests
import sqlite3
import time
import json
import sys
import os


class DB:
    def __init__(self, db_name):
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.database_path = os.path.join(self.path, db_name)
        self.old_database_path = self.database_path.replace('db', 'db_old')
        if exists(self.old_database_path):
            os.remove(self.old_database_path)
        if exists(self.database_path):
            os.rename(self.database_path, self.old_database_path)
            #old_con = sqlite3.connect(old_database_path)
            #old_cur = old_con.cursor()
        self.con = sqlite3.connect(self.database_path)
        self.cur = self.con.cursor()

    def update(self):
        self.cur.execute('CREATE TABLE IF NOT EXISTS people \
            (firstname TEXT NOT NULL DEFAULT "", \
             lastname TEXT NOT NULL DEFAULT "", \
             sex TEXT, \
             birthdate DATE, \
             birthplace TEXT, \
             birthsource TEXT, \
             deathdate DATE, \
             deathplace TEXT, \
             deathsource TEXT, \
             note TEXT, \
             permalink TEXT PRIMARY KEY, \
             family_id INT, \
             timecode TEXT, \
             source TEXT, \
             id_gramps TEXT, \
             CONSTRAINT `unique_permalink` UNIQUE(permalink) ON CONFLICT REPLACE)'
             )
        self.cur.execute('CREATE TABLE IF NOT EXISTS family \
            (id TEXT PRIMARY KEY, \
             father_permalink TEXT, \
             mother_permalink TEXT, \
             wedding_date DATE, \
             wedding_place TEXT, \
             source TEXT, \
             CONSTRAINT `unique_id` UNIQUE(id) ON CONFLICT REPLACE)')
        self.con.commit()

    def compare(self):
        self.con.execute("ATTACH ? AS dbold", [self.old_database_path])

        res1 = self.con.execute("""SELECT * FROM people INTERSECT \
            SELECT * FROM dbold.people
            """).fetchall()
        res2 = self.con.execute("""SELECT * FROM people EXCEPT \
            SELECT * FROM dbold.people
            """).fetchall()


class Family:
    instances = {}

    def __init__(self, father_permalink, mother_permalink):
        self.id = father_permalink + '#' + mother_permalink
        self.father_permalink = father_permalink
        self.mother_permalink = mother_permalink
        self.wedding_date = self.wedding_place = ''
        self.source = ''
        Family.instances[self.id] = self

    def get(father_permalink, mother_permalink):
        family_id = father_permalink + '#' + mother_permalink
        return Family.instances[family_id] if family_id in Family.instances.keys() else Family(father_permalink, mother_permalink)

    def save(self, DB):
        DB.cur.execute('INSERT INTO family (id, father_permalink, mother_permalink, wedding_date, wedding_place, source) VALUES (?, ?, ?, ?, ?, ?)',
                       (self.id, self.father_permalink, self.mother_permalink, self.wedding_date, self.wedding_place, self.source))


class People:
    def __init__(self,
                 permalink,
                 firstname='',
                 lastname='',
                 sex='',
                 birthdate='',
                 birthplace='',
                 deathdate='',
                 deathplace='',
                 family_id='',
                 timecode_id='',
                 note='',
                 birthsource='',
                 deathsource='',
                 id_gramps=''):
        self.permalink = permalink
        self.firstname = firstname
        self.lastname = lastname
        self.sex = sex
        self.birthdate = birthdate
        self.birthplace = birthplace
        self.deathdate = deathdate
        self.deathplace = deathplace
        self.permalink = permalink
        self.family_id = family_id
        self.timecode = timecode_id
        self.note = note
        self.birthsource = birthsource
        self.deathsource = deathsource
        self.id_gramps = id_gramps

    def __str__(self):
        return ' '.join(map(str, (self.sex, self.firstname, self.lastname, self.permalink, self.birthdate, self.birthplace, self.birthsource, self.deathdate, self.deathplace, self.deathsource, self.note, self.id_gramps)))

    def save(self, DB):
        DB.cur.execute('INSERT INTO people (firstname, lastname, sex, birthdate, birthplace, birthsource, deathdate, deathplace, deathsource, timecode, note, permalink, id_gramps) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                       (self.firstname, self.lastname, self.sex, self.birthdate, self.birthplace, self.birthsource, self.deathdate, self.deathplace, self.deathsource, self.timecode, self.note, self.permalink, self.id_gramps))


class Process:

    def __init__(self, filename, DB, base='http://roglo.eu/roglo?'):
        self.filename = filename
        self.cache = {}
        self.DB = DB
        self.base = base

    def init_caches(self):
        if not len(self.cache) and os.path.isfile(self.filename) and os.path.getmtime(self.filename) > time.time() - 12 * 3600 and os.path.getsize(self.filename) > 0:
            with open(self.filename, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                self.DB.cur.execute(
                    'SELECT firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id, timecode, note, birthsource, deathsource, id_gramps FROM people')
                for (firstname, lastname, sex, birthdate, birthplace, deathdate, deathplace, permalink, family_id, timecode, note, birthsource, deathsource, id_gramps) in DB.cur.fetchall():
                    people = People(permalink, firstname, lastname, sex,
                                    birthdate, birthplace, deathdate,
                                    deathplace, family_id, timecode, note, birthsource, deathsource, id_gramps)
                    for (k, v) in cache.items():
                        if v == people.permalink:
                            self.cache[k] = people
                            continue

    def save_caches(self):
        with open(self.filename, 'w') as f:
            for (k, v) in self.cache.items():
                if isinstance(v, str):
                    self.cache[k] = v
                else:
                    self.cache[k] = v.permalink
            json.dump(self.cache, f)

    def extractParams(self, href):
        str1 = self.extractQuery(href)
        return {x[0]: x[1] for x in [x.split("=") for x in str1[1:].split(";")]}

    def extractQuery(self, href):
        parts = href.split('?')
        return parts[1] if len(parts) > 1 else ''

    def dictToDate(self, d):
        if 'yg' in d.keys() and 'mg' in d.keys() and 'dg' in d.keys():
            return d['yg'] + '-' + d['mg'].zfill(2) + '-' + d['dg'].zfill(2)
        elif 'yg' in d.keys():
            return d['yg']
        return ''

    def browse(self, path, id_gramps=''):
        time.sleep(2)
        response = requests.get(self.base + path,
                                headers={'User-Agent': 'Mozilla/5.0 \
                                    (Windows NT 6.1; WOW64; rv:20.0) \
                                    Gecko/20100101 Firefox/20.0',
                                         'Accept': 'text/html',
                                         'Accept-Charset': 'utf-8',
                                         'Accept-Encoding': 'UTF-8',
                                         'Accept-Language': 'fr-FR,fr;q=0.8'
                                         })

        parts = response.text.split('<h3')

        soup = BeautifulSoup(parts[0], "html.parser")
        if soup.select('head meta[name="robots"]')[0].text != '':
            print(soup.select('head meta[name="robots"]')[0].text)
            warn("Access refused adress has been considered to be a robot")
            return
        # Parsing Person

        permalink_ = soup.select('h1 input')[0]['value'].strip() if len(
            soup.select('h1 input')) > 0 else ''
        parts = permalink_.replace('[', '').replace(']', '').split('/')
        if not parts[-1] == 'x x':
            permalink = ('p=%s;n=%s;' % (parts[0], parts[1]) + ('oc=%s' % (
                parts[2],) if parts[2] != '0' else '')) if len(parts) > 2 else ''
            people = People(permalink)
            people.sex = [i['alt'].strip() for i in soup.select('h1 img[alt]') if i['alt'].strip() in ['H', 'F']][0] if len(
                soup.select('h1 img')) > 0 else ''
            people.firstname = [i for i in soup.select('h1 a[href]') if i['href'].startswith('roglo?lang=fr;m=P')][0].text.strip() if len(
                soup.select('h1 a')) > 0 else ''
            people.lastname = [i for i in soup.select('h1 a[href]') if i['href'].startswith('roglo?lang=fr;m=N')][0].text.strip() if len(
                soup.select('h1 a')) > 1 else ''
            dict1 = self.extractParams(soup.select('ul li a.date')[
                0]['href'].strip()) if len(soup.select('ul li a.date')) > 0 else {}
            people.birthdate = self.dictToDate(dict1)
            dict2 = self.extractParams(soup.select('ul li a.date')[
                1]['href'].strip()) if len(soup.select('ul li a.date')) > 1 else {}
            people.deathdate = self.dictToDate(dict2)
            people.birthplace = soup.select('ul li script')[0].text.strip().split('"')[
                1] if len(soup.select('ul li script')) > 0 else ''
            people.deathplace = soup.select('ul li script')[1].text.strip().split('"')[
                1] if len(soup.select('ul li script')) > 1 else ''

            soup = BeautifulSoup(response.text, "html.parser")
            note = [i.string for i in soup.select(
                'body ul li') if not i.has_attr("date") and i.string]
            note.extend([i.text.strip() for i in soup.select('body dl dd')])
            people.note = '\r\n'.join(note)
            people.deathsource = '\r\n'.join([i.text for i in soup.select(
                'body p em br') if "décès" in i.text])
            people.birthsource = '\r\n'.join([i.text for i in soup.select(
                'body p em br') if "naissance" in i.text])
            _, * \
                temptimecode = soup.select(
                    'tr > td > span')[-1].text.strip().rsplit(' ', 5)
            people.timecode = ' '.join(temptimecode)
            people.id_gramps = id_gramps
            print(people)
            people.save(self.DB)
            self.cache[path] = people
            self.DB.con.commit()

            # Parsing Parents
            parents = soup.find('h3', text='Parents')
            if parents:
                ul = parents.findNext('ul')
                links = ul.findAll('li')
                father_ = self.extractQuery(links[0].find(
                    'a')['href'].strip()) if len(links) > 0 else ''
                if father_:
                    father = self.cache[father_] if father_ in self.cache.keys(
                    ) else self.browse(father_)
                father_permalink = father.permalink if father else ''
                mother_ = self.extractQuery(links[1].find(
                    'a')['href'].strip()) if len(links) > 1 else ''
                if mother_:
                    mother = self.cache[mother_] if mother_ in self.cache.keys(
                    ) else self.browse(mother_)
                mother_permalink = mother.permalink if mother else ''
                if father_permalink or mother_permalink:
                    family = Family.get(father_permalink, mother_permalink)
                    family.save(self.DB)
                    self.DB.cur.execute(
                        'UPDATE people SET family_id = ? WHERE permalink = ?', (family.id, people.permalink))
            spouses = soup.find('h3', text='Spouses and children') or soup.find(
                'h3', text='Mariages et enfants')
            # Parsing Spouses and childrens
            if spouses:
                ul = spouses.findNext('ul')
                links = ul.findAll('b')
                spouse_ = self.extractQuery(links[0].find(
                    'a')['href'].strip()) if len(links) > 0 else ''
                if spouse_ and spouse_ not in self.cache.keys():
                    spouse = self.browse(spouse_)
                    if spouse:
                        dict1 = self.extractParams(ul.select('li a.date')[0]['href'].strip()) if len(
                            ul.select('li a.date')) > 0 else {}
                        wedding_date = self.dictToDate(dict1)
                        wedding_place = ul.select('li script')[0].text.strip().split('"')[
                            1] if len(ul.select('li script')) > 0 else ''
                        father_permalink = people.permalink if people.sex == 'H' else spouse.permalink
                        mother_permalink = spouse.permalink if spouse.sex == 'F' else people.permalink
                        family = Family.get(father_permalink, mother_permalink)
                        family.wedding_date = wedding_date
                        family.wedding_place = wedding_place
                        family.source = '\r\n'.join([i.text for i in soup.select(
                            'body p em br') if "famille" in i.text or "mariage" in i.text])
                        family.save(self.DB)
                        print('W %s %s %s' %
                              (wedding_date, wedding_place, family.id))

        else:
            people = None
        return people

    def export(self, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            # Write people
            self.DB.cur.execute(
                'SELECT firstname, lastname, sex, birthdate, birthplace, birthsource, deathdate, deathplace, deathsource, permalink, family_id, timecode, note, id_gramps FROM people')
            f.write(
                'person,grampsid,firstname,lastname,gender,note,birthdate,birthplace,birthsource,deathdate,deathplace,deathsource,attributetype,attributevalue\n')
            people = self.DB.cur.fetchall()
            for (firstname, lastname, sex, birthdate, birthplace, birthsource, deathdate, deathplace, deathsource, permalink, family_id, timecode, note, id_gramps) in people:
                sex = 'male' if sex == 'M' else 'female' if sex == 'F' else ''
                source_adress = self.base + permalink
                f.write('{0},{12},"{1}","{2}",{3},"{4}",{5},"{6}","{7}",{8},"{9}","{10}", Roglo, "{11}"\n'.format(
                    permalink, firstname, lastname, sex, note, birthdate, birthplace, birthsource, deathdate, deathplace, deathsource, source_adress, id_gramps))
            # Write marriage
            f.write('\n\nmarriage,husband,wife,date,place,source\n')
            self.DB.cur.execute(
                'SELECT id, father_permalink, mother_permalink, wedding_date, wedding_place FROM family')
            for (family_id, father_permalink, mother_permalink, wedding_date, wedding_place) in DB.cur.fetchall():
                f.write('%s,%s,%s,%s,"%s"\n' % (family_id, father_permalink,
                        mother_permalink, wedding_date or '', wedding_place or ''))
            # Write family
            f.write('\n\nfamily,child\n')
            for (firstname, lastname, sex, birthdate, birthplace, birthsource, deathdate, deathplace, deathsource, permalink, family_id, timecode, note, id_gramps) in people:
                f.write('%s,%s\n' % (family_id or '', permalink))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--site', '-s', type=str, default="roglo")
    parser.add_argument('--cache', '-c', type=str, default="cache.json")
    parser.add_argument('--export', '-e', type=str, default="export.csv")
    parser.add_argument('--list_adress', '-la', nargs='+', type=str, default="")
    parser.add_argument('--list_gramps', '-g', nargs='+', type=str, default="")
    parser.add_argument('--database', '-d', type=str, default="db_test.sqlite3")
    args = parser.parse_args()
    DB = DB(args.database)
    DB.update()
    process = Process(args.cache, DB)
    process.init_caches()
    if len(args.list_adress) > 1:
        for i_url in range(len(args.list_adress)):
            url = args.list_adress[i_url]

            process.browse(url.replace(process.base, ''), args.list_gramps[i_url])
            DB.con.commit()
            process.save_caches()

        process.export(args.export)
    else:
        print('Please provide a URL')
