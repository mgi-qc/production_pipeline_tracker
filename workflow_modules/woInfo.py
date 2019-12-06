import subprocess
import os
import sys
import csv


class woInfo:

    def __init__(self, woid):

        self.woid = woid

    def get_admin(self):

        admin_query_dict = {'Work Order ID': self.woid,
                            'Administration Project': 'NA',
                            'Pipeline': 'NA',
                            'Description': 'NA',
                            'Status': 'NA',
                            'Facilitator': 'NA',
                            'Billing Account': 'NA',
                            'WO Start Date': 'NA',
                            'Creator': 'NA',
                            'Facilitator Comment': 'NA',
                            'Is For CLE?': 'NA',
                            }

        admin_collections = subprocess.check_output(["wo_info", "--report", "billing", "--woid", self.woid]).decode(
            'utf-8').splitlines()

        if not admin_collections:
            sys.exit('wo_info query failed for {}'.format(self.woid))

        if admin_collections:

            for line in admin_collections:

                if 'Administration Project' in line:
                    admin_query_dict['Administration Project'] = str(line.split(':')[1].strip())

                if 'Pipeline' in line:
                    admin_query_dict['Pipeline'] = str(line.split(':')[1].strip())

                if 'Description' in line:
                    admin_query_dict['Description'] = str(line.split(':')[1].strip())

                if 'Status' in line:
                    admin_query_dict['Status'] = str(line.split(':')[1].strip())

                if 'WO Facilitator' in line:
                    admin_query_dict['Facilitator'] = str(line.split(':')[1].strip())

                if 'Billing Acct Name' in line:
                    admin_query_dict['Billing Account'] = str(line.split(':')[1].strip())

                if 'WO Start Date' in line:
                    admin_query_dict['WO Start Date'] = str(line.split(':')[1].strip())

                if 'Creator' in line:
                    admin_query_dict['Creator'] = str(line.split(':')[1].strip())

                if 'Facilitator Comment' in line:
                    admin_query_dict['Facilitator Comment'] = str(line.split(':')[1].strip())

                if 'Is For CLE?' in line:
                    admin_query_dict['Is For CLE?'] = str(line.split(':')[1].strip())

        return admin_query_dict

    def get_samples(self):

        if os.path.isfile('sample.tsv'):
            os.remove('sample.tsv')

        subprocess.check_output(["wo_info", "--report", "sample", "--woid", self.woid, "--format", "tsv"]).decode(
            'utf-8').splitlines()

        if not os.path.isfile('sample.tsv'):
            print('Skipping {}, sample.tsv file failed to create'.format(self.woid))
            return False

        with open('sample.tsv', 'r') as f, open('{}.sample.tsv'.format(self.woid), 'w') as w:
            for line in f:
                if '----' in line or 'Sample Overview' in line:
                    continue
                w.write(line)

        with open('{}.sample.tsv'.format(self.woid), 'r') as f:
            data = {}
            f_reader = csv.DictReader(f, delimiter='\t')

            for line in f_reader:
                if line['Sample Full Name'] not in data:
                    data[line['Sample Full Name']] = line
                else:
                    sys.exit('{} sample is duplicate'.format(line['Sample Full Name']))

        return data


