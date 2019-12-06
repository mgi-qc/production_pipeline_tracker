import csv
import os
import datetime
import subprocess


class SequencingUpdate:

    mss_required_columns = ['Work Order ID',
                            'Current Pipeline Status',
                            'Current Production Status',
                            'WOI Status',
                            'Sample Full Name',
                            'Sequencing Completed Date']

    def illumina_info(self, woid):

        date = datetime.datetime.now().strftime("%m%d%y")
        hour_min = datetime.datetime.now().strftime("%H%M")
        outfile_temp = 'illumina_info_temp.tsv'
        outfile = 'library_index_summary.{}.{}.tsv'.format(hour_min, date)
        failed_file = 'library_index_summary.totalkb.fail.{}.{}.tsv'.format(hour_min, date)
        woid_arg = '--woid={}'.format(woid)

        subprocess.run(
            ['illumina_info', '-report', 'library_index_summary', '--format', 'tsv', woid_arg, '--incomplete',
             '--output-file-name', outfile_temp])

        if not os.path.isfile(outfile_temp):
            print('illumina_info failed to create {} file'.format(outfile))
            return False, False

        with open(outfile_temp, 'r') as f, open(outfile, 'w') as o:

            for line in f:
                if line and '--' not in line and 'Library Index Summary' not in line:
                    o.write(line)

        os.remove(outfile_temp)

        with open(outfile, 'r') as f, open(failed_file, 'w') as fail, open(outfile_temp, 'w') as oft:
            fh = csv.DictReader(f, delimiter='\t')
            temp_writer = csv.DictWriter(oft, fieldnames=fh.fieldnames, delimiter='\t')
            temp_writer.writeheader()
            fail_writer = csv.DictWriter(fail, fieldnames=fh.fieldnames, delimiter='\t')
            fail_writer.writeheader()

            data = {}
            fails_found = False
            failed_samples = []

            for line in fh:
                if float(line['Total Bases Kb (PF)'].replace(',', '')) >= 63000000:
                    data[line['Library'].split('-lib')[0]] = line
                    temp_writer.writerow(line)
                else:
                    fails_found = True
                    fail_writer.writerow(line)
                    failed_samples.append('{}\t{}'.format(line['Library'].split('-lib')[0],line['Total Bases Kb (PF)']))

        os.replace(outfile_temp, outfile)

        if fails_found:
            print('Samples failed to meet total bases threshold: {}'.format(len(failed_samples)))
            print('\n'.join(failed_samples))
            print('\nFailed sample file: {}'.format(failed_file))

        else:
            print('No samples failed to meet total bases threshold.')
            os.remove(failed_file)

        print('Report outfile: {}\n'.format(outfile))

        return data, outfile

    def update_mss_sheet(self, ss_conn, admin_info, sample_data, sheet_info_dict):

        total_samples = len(sample_data)
        total_updated_samples = 0
        update_sample_name = []

        for folder, sheet_info in sheet_info_dict.items():

            for sheet_name, sheet_id in sorted(sheet_info.items()):

                mss_update_col = []
                updated_rows = []

                match = []
                fail = []

                updated_samples = 0

                sheet_col_ids = ss_conn.get_column_ids(sheet_id)

                # get required column id's to pull from sheet
                for col_title, col_id in sheet_col_ids.items():
                    if col_title in self.mss_required_columns:
                        mss_update_col.append(col_id)

                mss_sheet = ss_conn.get_sheet_with_columns(sheet_id=sheet_id, column_list=mss_update_col)

                for row in mss_sheet.rows:

                    sample_found = False
                    swoid_found = False

                    for cell in row.cells:

                        if cell.column_id == sheet_col_ids['Work Order ID']:
                            swoid = cell.value
                            for k, v in sample_data.items():
                                if str(swoid) in sample_data[k]['WorkOrder']:
                                    swoid_found = True

                        if cell.column_id == sheet_col_ids['Sample Full Name']:
                            sample_name = cell.value
                            if sample_name in sample_data.keys():
                                sample_found = True

                        if cell.column_id == sheet_col_ids['Current Production Status']:
                            sample_production_status = cell.value

                    if sample_found and swoid_found:

                        if sample_production_status != 'Sequence Complete':
                            updated_samples += 1

                            new_row = ss_conn.smart_sheet_client.models.Row()
                            new_row.id = row.id

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Current Production Status']
                            new_cell.value = 'Sequencing Completed'
                            new_row.cells.append(new_cell)

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Sequencing Completed Date']
                            new_cell.value = datetime.datetime.now().isoformat()
                            new_row.cells.append(new_cell)

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['Current Pipeline Status']
                            new_cell.value = admin_info['Pipeline']
                            new_row.cells.append(new_cell)

                            new_cell = ss_conn.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_col_ids['WOI Status']
                            new_cell.value = admin_info['Status']
                            new_row.cells.append(new_cell)

                            updated_rows.append(new_row)
                            update_sample_name.append(sample_name)

                            continue

                update = ss_conn.smart_sheet_client.Sheets.update_rows(mss_sheet.id, updated_rows)
                print(sheet_name)
                print('Sequence complete samples to update: {}'.format(total_samples))
                print('Samples updated: {}'.format(updated_samples))
                print('Samples remaining: {}\n'.format(total_samples - updated_samples))

                total_samples = total_samples - updated_samples
                total_updated_samples += updated_samples

        for sample in sample_data:
            if sample not in update_sample_name:
                print(sample)
        return total_updated_samples

    def pcc_find_sibling_id(self, pccs, woid, admin_project):

        admin_id = False
        sibling_id = False
        sequencing_complete = False

        for row in pccs[0].rows:
            for cell in row.cells:

                if cell.column_id == pccs[1]['Reporting Instance'] and cell.value == 'Sequencing Completed':
                    sequencing_complete = True

                if sequencing_complete and cell.column_id == pccs[1]['Admin Project'] and cell.value == admin_project:
                    admin_id = row.id

                if sequencing_complete and cell.column_id == pccs[1]['Reporting Instance'] and cell.value == woid:
                    sibling_id = row.id

                if cell.column_id == pccs[1]['Reporting Instance'] and cell.value == 'QC Reports':

                    if sibling_id:
                        return sibling_id
                    if admin_id and not sibling_id:
                        return admin_id
        return False

    def pcc_update(self, ss_connector, pccs, woid, admin_info, sample_number, attachment):

        # find last sibling row, insert row under sibling.

        sibling_id = self.pcc_find_sibling_id(pccs=pccs, woid=woid, admin_project=admin_info['Administration Project'])

        print('Updating Production Communications Sheet')
        sheet, col_dict = pccs

        for row in sheet.rows:
            for cell in row.cells:
                # find 'qPCR dilution drop-off' line, capture row id for parent
                if cell.column_id == col_dict['Reporting Instance'] and cell.value == \
                        'Sequence Complete':
                    sequencing_parent_id = row.id
                    break

        new_swo_row = ss_connector.smart_sheet_client.models.Row({"format": ",,,,,,,,,18,,,,,,"})
        new_swo_row.cells.append({'column_id': col_dict['Reporting Instance'], 'value': woid, 'hyperlink': {
            'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(woid)}})
        new_swo_row.cells.append({'column_id': col_dict['Sequencing Work Order'], 'value': woid})
        new_swo_row.cells.append({'column_id': col_dict['Items'], 'value': sample_number})
        new_swo_row.cells.append(
            {'column_id': col_dict['Admin Project'], 'value': admin_info['Administration Project'], 'hyperlink': {
                'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'.format(
                    admin_info['Administration Project'].replace(' ', '+'))}})
        new_swo_row.cells.append(
            {'column_id': col_dict['Facilitator'], 'value': admin_info['user email']})
        new_swo_row.cells.append({'column_id': col_dict['Event Date'], 'value': datetime.datetime.now().isoformat()})
        new_swo_row.cells.append({'column_id': col_dict['Production Notes'], 'value': admin_info['Description']})

        if sibling_id:
            new_swo_row.sibling_id = sibling_id
            new_swo_row.below = True
        if not sibling_id:
            new_swo_row.parent_id = sequencing_parent_id
            new_swo_row.to_bottom = True

        new_title_row_response = ss_connector.smart_sheet_client.Sheets.add_rows(sheet.id, [new_swo_row])

        for r in new_title_row_response.data:
            new_title_row_id = r.id

        # attach spreadsheet
        ss_connector.smart_sheet_client.Attachments.attach_file_to_row(sheet.id, new_title_row_id, (attachment, open(
            attachment, 'rb'), 'application/Excel'))

        comment = input('\nDilution Drop Off Comments (Enter to continue without comment):\n')
        # comment = 'Making some comments yo'
        if comment:
            ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(sheet.id, new_title_row_id,
                                                                                 ss_connector.smart_sheet_client.models.
                                                                                 Discussion({'comment': ss_connector.
                                                                                            smart_sheet_client.models.
                                                                                            Comment({'text': comment})})
                                                                                 )

        return




