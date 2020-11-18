import os
import sys
import csv
import subprocess
import smartsheet
import workflow_modules.woInfo as woinfo

# Smartsheet connection
ss_c = smartsheet.Smartsheet(os.environ.get('SMRT_API'))
ss_c.errors_as_exceptions()

confluence_sheet_id = 3521933800171396

col_ids = {}
for col in ss_c.Sheets.get_columns(confluence_sheet_id).data:
    col_ids[col.title] = col.id

reagent_col_ids = {}
for col in ss_c.Sheets.get_columns(7014945071097732).data:
    reagent_col_ids[col.title] = col.id


headers = ['Work Order ID', 'Pipeline', 'Administration Project', 'Description', 'Billing Account', 'Facilitator']
pipeline_color = {'IDT Exome Sequencing': '9', 'Illumina RNA Sequencing': '8', 'IDT Targeted Sequencing': '16',
                  'Illumina Whole Genome Sequencing': '6', 'Resource Storage': '4', 'PacBio': '7', 'NanoString': '10',
                  'Technology Development Resource Storage': '5', 'RNA Resource Storage': '10'}


def get_email_column_ids(sheet_column_object):
    column_id_dict = {}
    for col in sheet_column_object:
        column_id_dict[col.title] = col.id
    return column_id_dict


def update_admin_email(facilitator, s_con):

    user_sheet = s_con.Sheets.get_sheet(252568824768388)
    sheet_column_dict = get_email_column_ids(user_sheet.columns)
    found_user = False

    for row in user_sheet.rows:
        for cell in row.cells:
            if cell.column_id == sheet_column_dict['Name'] and facilitator == cell.value:
                found_user = True

            if cell.column_id == sheet_column_dict['User Name'] and facilitator == cell.value:
                found_user = True

            if found_user and cell.column_id == sheet_column_dict['Email Address']:
                return cell.value

    return 'pmgroup@gowustl.onmicrosoft.com'


class Confluence:

    def __init__(self, woid):

        self.woid = woid
        self.w = woinfo.woInfo(self.woid)

    def get_admin(self):

        # get admin_collections
        admin_info = self.w.get_admin()

        if not admin_info:
            sys.exit('{} returned no results, exiting'.format(self.woid))
        return admin_info

    def get_sample_number(self):
        return len(self.w.get_samples())

    def get_assay(self):

        db_wo = subprocess.check_output(['db', 'wo', self.woid]).decode('utf-8').splitlines()

        for l in db_wo:
            if 'assay_id' in l:
                assay_id = l.split()[-1]
                if len(assay_id) == 0:
                    return 'NA'
                break

        subprocess.run(['limfo', 'e', 'assay', '--assay-id', assay_id, '--format', 'tsv'])

        if not os.path.isfile('core_info.tsv'):
            return 'NA'

        with open('core_info.tsv', 'r') as ci:

            ci_reader = csv.reader(ci, delimiter='\t')

            header_found = False

            for l in ci_reader:

                if len(l) == 0 or l[0] in ['-', 'Entity']:
                    continue

                if '#' in l[0] and not header_found:
                    header = l
                    header_found = True
                    continue

                if header_found:
                    l_dict = dict(zip(header, l))
                    os.remove('core_info.tsv')
                    return l_dict['Assay']

        return 'NA'

    def add_confluence_row(self, woinfo_dict, sample_number=None):

        new_row = ss_c.models.Row()
        new_row.parent_id = 7516422195439492
        new_row.to_top = True

        new_reagent_row = ss_c.models.Row()
        new_reagent_row.parent_id = 597069978986372
        new_reagent_row.to_top = True

        if sample_number is None:
            sample_number = self.get_sample_number()

        new_row.cells.append({'column_id': col_ids['Assay'], 'value': self.get_assay()})
        new_row.cells.append({'column_id': col_ids['Active Items'], 'value': sample_number})

        new_reagent_row.cells.append({'column_id': reagent_col_ids['Assay'], 'value': self.get_assay()})
        new_reagent_row.cells.append({'column_id': reagent_col_ids['Active Items'], 'value': sample_number})

        for header, value in woinfo_dict.items():

            if header in headers:

                if header == 'user email':
                    continue

                if 'Work Order' in header:
                    new_row.cells.append({'column_id': col_ids[header], 'value': value,
                                          'hyperlink':
                                              {'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'
                                         .format(value)}})

                    new_reagent_row.cells.append({'column_id': reagent_col_ids[header], 'value': value,
                                                  'hyperlink':
                                                      {'url':
                                                           'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'
                                                 .format(value)}})
                    continue

                if header == 'Facilitator':
                    new_row.cells.append({'column_id': col_ids[header], 'value': update_admin_email(value, ss_c)})

                    new_reagent_row.cells.append({'column_id': reagent_col_ids[header], 'value': update_admin_email(
                        value, ss_c)})
                    continue

                if value in pipeline_color:
                    new_row.cells.append({'column_id': col_ids[header], 'value': value,
                                          "format": ",,,,,,,,,{},,,,,,".format(pipeline_color[value])})

                    new_reagent_row.cells.append({'column_id': reagent_col_ids[header], 'value': value, "format":
                                                  ",,,,,,,,,{},,,,,,".format(pipeline_color[value])})
                    continue

                new_row.cells.append({'column_id': col_ids[header], 'value': value})
                new_reagent_row.cells.append({'column_id': reagent_col_ids[header], 'value': value})

        con_woid_row_response = ss_c.Sheets.add_rows(confluence_sheet_id, [new_row])

        if woinfo_dict['Pipeline'] not in ['Resource Storage', 'PacBio', 'NanoString',
                                           'Technology Development Resource Storage', 'RNA Resource Storage']:
            print('Adding {} to Illumina and qPCR Reagents.'.format(self.woid))
            reagent_row_response = ss_c.Sheets.add_rows(7014945071097732, [new_reagent_row])
