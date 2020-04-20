import os
import sys
import csv
import subprocess
import datetime
from time import sleep


class Ddo:

    mss_required_columns = ['Fail',
                            'Work Order ID',
                            'Current Production Status',
                            'Sample Full Name',
                            'Sequencing Scheduled Date',
                            'Initial Sequencing Scheduled Date']

    def __init__(self, ddo_bc_infile):

        self.ddo_bc_file = ddo_bc_infile

    def get_bar_codes(self):

        bc_list = []
        with open(self.ddo_bc_file, 'r') as f:
            for line in f:
                bc_list.append(line.strip())
        return bc_list

    def run_bc_info(self):

        date = datetime.datetime.now().strftime("%m%d%y")
        hour_min = datetime.datetime.now().strftime("%H%M")
        outfile = 'sequencing_scheduled_ddo.{}.{}.tsv'.format(hour_min, date)

        print('\nEmailing qPCR dropoff report.')
        subprocess.run(["barcode_info", "--r", "ddo", "--bc-file", self.ddo_bc_file, "--format", "email"])

        print("Generating qpcr dropoff report tsv.\n")
        subprocess.run(
            ["barcode_info", "--r", "ddo", "--bc-file", self.ddo_bc_file, "--format", "tsv", "--output-file-name",
             outfile])

        return outfile

    def get_samples(self, barcode_list):

        samples = []
        date = datetime.datetime.now().strftime("%m%d%y")
        hour_min = datetime.datetime.now().strftime("%H%M")
        outfile = 'sequencing_sample_inventory_ddo.{}.{}.tsv'.format(hour_min, date)

        subprocess.run(['barcode_info', '--report', 'sample_inventory', '--bc', ','.join(barcode_list),
                        '--format', 'tsv', "--output-file-name", outfile])

        if not os.path.isfile(outfile):
            sys.exit('{} sample file not found'.format(outfile))

        with open(outfile, 'r') as f:
            next(f)
            fh = csv.DictReader(f, delimiter='\t')
            for line in fh:
                if '--' not in line['DNA']:
                    if 'Pooled_Library' not in line['DNA']:
                        samples.append(line['DNA'])

        return samples, outfile

    def get_woids(self, infile):

        bc_dict_data = {}
        header_line = False
        woids = list()

        with open(infile, 'r') as f:

            fh = csv.reader(f, delimiter='\t')

            for line in fh:

                if len(line) == 1 and 'Dilution Drop Off' in line[0]:
                    dilution_drop_off = line[0]

                if '#' in line and not header_line:
                    header_line = True
                    header = line
                    continue

                # updated header line processing
                # if len(line) > 1 and 'Sources' in line[1] and header_line:
                if len(line) > 1 and header_line and '-' not in line[0]:
                    line_dict = {k: v for k, v in zip(header, line)}
                    bc_dict_data[line_dict['Barcode']] = {'items': line_dict['Index'].split()[0], 'admin': set(),
                                                          'woids': line_dict['Outgoing Queue Work Order'].split(','),
                                                          'dilution_drop_off': dilution_drop_off, 'facilitator':
                                                              ','.join(set(
                                                                  line_dict['Outgoing Queue Work Order Facilitator'].
                                                                  split(',')))}

                    woid_list = line_dict['Outgoing Queue Work Order'].split(',')
                    woids.extend(woid_list)

                if len(line) == 1 and 'Parents' in line[0]:
                    return list(set(woids)), bc_dict_data

        return list(set(woids)), bc_dict_data

    def sample_update(self, ss_conn, sample_sheet_dict, woids, samples, date, status, fails):

        woid_fail_sample_count = {}

        date_column_name = 'Sequencing Scheduled Date'
        if 'Initial' in status:
            date_column_name = 'Initial Sequencing Scheduled Date'

        updated_sample_count = 0
        for sheet_info in sample_sheet_dict.values():
            for sheet_name, id_ in sorted(sheet_info.items()):

                print('\nChecking samples in: {}'.format(sheet_name))
                mss_update_col = []
                updated_rows = []
                match = []
                fail = []

                sheet_col_ids = ss_conn.get_column_ids(id_)

                # get required column id's to pull from sheet
                for col_title, col_id in sheet_col_ids.items():
                    if col_title in self.mss_required_columns:
                        mss_update_col.append(col_id)

                mss_sheet = ss_conn.get_sheet_with_columns(sheet_id=id_, column_list=mss_update_col)

                for row in mss_sheet.rows:

                    row_woid = False
                    row_sample = False
                    qc_pass = False

                    for cell in row.cells:
                        if cell.column_id == sheet_col_ids['Work Order ID']:
                            rw = cell.value
                            if rw in woids:
                                row_woid = True

                        if cell.column_id == sheet_col_ids['Sample Full Name']:
                            rs = cell.value
                            if cell.value in samples:
                                row_sample = True

                        if cell.column_id == sheet_col_ids['Current Production Status']:
                            if cell.value == 'QC Pass':
                                qc_pass = True

                    if row_woid and row_sample and not qc_pass:

                        match.append('True: {} {}'.format(rw, rs))

                        new_row = ss_conn.smart_sheet_client.models.Row()
                        new_row.id = row.id

                        new_cell = ss_conn.smart_sheet_client.models.Cell()
                        new_cell.column_id = sheet_col_ids[date_column_name]
                        new_cell.value = date
                        new_row.cells.append(new_cell)

                        failed_status = False

                        if rs in fails and fails[rs] in woids:

                            if rw not in woid_fail_sample_count:
                                woid_fail_sample_count[rw] = 1
                            else:
                                woid_fail_sample_count[rw] += 1

                            failed_status = True

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Fail']
                            new_cell.value = True
                            new_row.cells.append(new_cell)

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Current Production Status']
                            new_cell.value = 'qPCR Failed'
                            # turned off red cell color
                            # new_cell.format_ = ",,,,,,,,,27,,,,,,"
                            new_row.cells.append(new_cell)

                        if not failed_status:
                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Current Production Status']
                            new_cell.value = status
                            new_row.cells.append(new_cell)

                        updated_rows.append(new_row)
                    else:
                        fail.append('Fail: {} {}'.format(rw, rs))

                update = ss_conn.smart_sheet_client.Sheets.update_rows(mss_sheet.id, updated_rows)
                print('Updated Samples: {}'.format(len(match)))
                updated_sample_count += len(match)

            return woid_fail_sample_count, updated_sample_count

    def dup_pcc_bc_check(self, pcc_sheet_col_dict, barcode_dict):
        dup_bc = ''
        for row in pcc_sheet_col_dict[0].rows:
            for cell in row.cells:
                if cell.column_id == pcc_sheet_col_dict[1]['Reporting Instance'] and cell.value in barcode_dict.keys():
                    return True, cell.value
        return False, dup_bc

    def pcc_update(self, ss_connector, pcc_sheet_col_dict, barcode_dict, date, status, attachment, failures,
                   total_samples, fail_infile):

        print('\nUpdating Production Communications Sheet with:\n{}'.format('\n'.join(barcode_dict.keys())))

        ddo_title = barcode_dict[list(barcode_dict.keys())[0]]['dilution_drop_off']

        sheet, col_dict = pcc_sheet_col_dict

        sequencing_platform = input('\nPlease input sequencing platform:\n')
        # sequencing_platform = 'NovaSeq S4 (300C)'

        # iterate over sheet
        for row in sheet.rows:
            for cell in row.cells:
                # find 'qPCR dilution drop-off' line, capture row id for parent
                if cell.column_id == col_dict['Reporting Instance'] and cell.value == \
                        'qPCR results/Sequencing Scheduling':
                    qpcr_parent = row.id
                    break

        # create row with dilution drop off title, capture row id for parent
        new_title_row = ss_connector.smart_sheet_client.models.Row({"format": ",,,,,,,,,18,,,,,,"})
        new_title_row.cells.append(
            {'column_id': col_dict['Reporting Instance'], 'value': ddo_title})
        new_title_row.cells.append({'column_id': col_dict['Items'], 'value': total_samples})
        new_title_row.cells.append({'column_id': col_dict['Failure'], 'value': failures})
        new_title_row.to_bottom = True
        new_title_row.parent_id = qpcr_parent

        new_title_row_response = ss_connector.smart_sheet_client.Sheets.add_rows(sheet.id, [new_title_row])

        for r in new_title_row_response.data:
            new_title_row_id = r.id

        # attach spreadsheet
        ss_connector.smart_sheet_client.Attachments.attach_file_to_row(sheet.id, new_title_row_id, (attachment, open(
            attachment, 'rb'), 'application/Excel'))

        # attach fail file
        if fail_infile:
            sleep(10)
            ss_connector.smart_sheet_client.Attachments.attach_file_to_row(sheet.id, new_title_row_id,
                                                                           (fail_infile, open(
                                                                               fail_infile, 'rb'), 'application/Excel'))

        comment = input('\nDilution Drop Off Comments (Enter to continue without comment):\n')
        # comment = 'Making some comments yo'
        if comment:
            ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(sheet.id, new_title_row_id,
                                                                                 ss_connector.smart_sheet_client.models.
                                                                                 Discussion({'comment': ss_connector.
                                                                                            smart_sheet_client.models.
                                                                                            Comment({'text': comment})})
                                                                                 )
        # create row for each barcode assign title row as parent
        new_bc_rows = []
        for bc in barcode_dict:
            new_bc_row = ss_connector.smart_sheet_client.models.Row()
            new_bc_row.cells.append({'column_id': col_dict['Reporting Instance'], 'value': bc, 'hyperlink': {
                'url': 'https://imp-lims.ris.wustl.edu/entity/container/{}'.format(bc)}})
            new_bc_row.cells.append(
                {'column_id': col_dict['Sequencing Work Order'], 'value': ','.join(barcode_dict[bc]['woids'])})
            new_bc_row.cells.append({'column_id': col_dict['Items'], 'value': barcode_dict[bc]['items']})
            new_bc_row.cells.append({'column_id': col_dict['Admin Project'],
                                     'value': ','.join(barcode_dict[bc]['admin']), 'format': ",,,,,,,,,,,,,,,1,"})
            # new_bc_row.cells.append({'column_id': col_dict['Facilitator'],
            #                          'object_value': barcode_dict[bc]['facilitator']})
            new_bc_row.cells.append({'column_id': col_dict['Facilitator'], 'objectValue': {
                                     'objectType': 'MULTI_CONTACT', 'values': barcode_dict[bc]['user email']}})
            new_bc_row.cells.append({'column_id': col_dict['Event Date'], 'value': date})
            new_bc_row.cells.append({'column_id': col_dict['instrument/sequencing platform'],
                                     'value': sequencing_platform})
            new_bc_row.cells.append({'column_id': col_dict['Production Notes'], 'value': status})
            # new_bc_row.cells.append({'column_id': col_dict[''], 'value': ''})
            new_bc_row.to_bottom = True
            new_bc_row.parent_id = new_title_row_id

            new_bc_rows.append(new_bc_row)
        ss_connector.smart_sheet_client.Sheets.add_rows(sheet.id, new_bc_rows)
        return

    def pcc_fails_update(self, ss_connector, pcc_sheet_col_dict, fail_dict):

        """Currently not using this function, update if ressurected"""

        sheet, col_dict = pcc_sheet_col_dict

        for row in sheet.rows:
            fail_total = 0
            fail_true = False
            for cell in row.cells:
                # get row id for parent project
                if cell.column_id == col_dict['Sequencing Work Order']:
                    if cell.value:
                        woids = cell.value
                        woids = woids.split(',')
                        for woid in woids:
                            if woid in fail_dict.keys():
                                fail_true = True
                                fail_total += fail_dict[woid]

            if fail_true:
                new_row = ss_connector.smart_sheet_client.models.Row()
                new_row.id = row.id

                new_cell = ss_connector.smart_sheet_client.models.Cell()
                new_cell.column_id = col_dict['Failure']
                new_cell.value = fail_total
                new_row.cells.append(new_cell)

                ss_connector.smart_sheet_client.Sheets.update_rows(sheet.id, [new_row])
        return

