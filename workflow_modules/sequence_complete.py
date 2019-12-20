import csv
import os
import datetime
import subprocess
from time import sleep


class SequencingUpdate:

    mss_required_columns = ['Work Order ID',
                            'Pipeline',
                            'Current Production Status',
                            'WOI Status',
                            'Sample Full Name',
                            'Sequencing Completed Date']

    def illumina_info(self, woid, cutoff):

        date = datetime.datetime.now().strftime("%m%d%y")
        hour_min = datetime.datetime.now().strftime("%H%M")
        outfile_temp = 'illumina_info_temp.tsv'
        illumina_file = 'library_index_summary.totalkb.{}.{}.tsv'.format(hour_min, date)
        pass_file = 'library_index_summary.totalkb.pass.{}.{}.tsv'.format(hour_min, date)
        failed_file = 'library_index_summary.totalkb.fail.{}.{}.tsv'.format(hour_min, date)
        woid_arg = '--woid={}'.format(woid)

        attachment_files = []

        subprocess.run(['illumina_info', '-report', 'library_index_summary', '--format', 'email', woid_arg,
                        '--incomplete'])
        subprocess.run(
            ['illumina_info', '-report', 'library_index_summary', '--format', 'tsv', woid_arg, '--incomplete',
             '--output-file-name', outfile_temp])

        if not os.path.isfile(outfile_temp):
            print('illumina_info failed to create {} file'.format(outfile_temp))
            return False, False, False

        with open(outfile_temp, 'r') as f, open(illumina_file, 'w') as o:

            for line in f:
                if line and '--' not in line and 'Library Index Summary' not in line:
                    o.write(line)

        os.remove(outfile_temp)
        attachment_files.append(illumina_file)
        with open(illumina_file, 'r') as f, open(failed_file, 'w') as fail, open(pass_file, 'w') as oft:
            fh = csv.DictReader(f, delimiter='\t')
            temp_writer = csv.DictWriter(oft, fieldnames=fh.fieldnames, delimiter='\t')
            temp_writer.writeheader()
            fail_writer = csv.DictWriter(fail, fieldnames=fh.fieldnames, delimiter='\t')
            fail_writer.writeheader()

            data = {}
            fails_found = False
            pass_found = False
            failed_samples_report = []
            failed_samples = []

            for line in fh:
                if float(line['Total Bases Kb (PF)'].replace(',', '')) >= cutoff:
                    pass_found = True
                    data[line['Library'].split('-lib')[0]] = line
                    temp_writer.writerow(line)
                else:
                    fails_found = True
                    fail_writer.writerow(line)
                    failed_samples.append(line['Library'].split('-lib')[0])
                    failed_samples_report.append('{}\t{}'.format(line['Library'].split('-lib')[0],
                                                                 line['Total Bases Kb (PF)']))
        if pass_found:
            attachment_files.append(pass_file)

        if fails_found:
            attachment_files.append(failed_file)
            print('Samples failed to meet total bases threshold: {}'.format(len(failed_samples_report)))
            print('\n'.join(failed_samples_report))

        else:
            print('\nNo samples failed to meet total bases threshold.')
            os.remove(failed_file)

        print('\nillumina_info report files:\n{}'.format('\n'.join(attachment_files)))

        return data, attachment_files, failed_samples

    def update_mss_sheet(self, ss_conn, admin_info, sample_data, sheet_info_dict, failed_samples):

        total_samples = len(sample_data)
        print('\nSequence complete samples from illumina_info report: {}'.format(total_samples))
        total_updated_samples = 0
        total_existing_seq_complete_samples = 0
        update_sample_name = []
        seq_complete_samples = []
        attachment_files = []
        for folder, sheet_info in sheet_info_dict.items():

            date = datetime.datetime.now().strftime("%m%d%y")
            outfile = '{}.sequence.scheduled.all.{}.tsv'.format(admin_info['Work Order ID'], date)
            outfile_new_samples = '{}.sequence.scheduled.new.{}.tsv'.format(admin_info['Work Order ID'], date)
            attachment_files.extend([outfile, outfile_new_samples])
            with open(outfile, 'w') as f, open(outfile_new_samples, 'w') as o_new:

                outfile_header = ['Work Order ID', 'Sample Full Name', 'Pipeline', 'Current Production Status',
                                  'Sequencing Completed Date']
                f_write = csv.DictWriter(f, fieldnames=outfile_header, delimiter='\t')
                f_write.writeheader()

                o_new_write = csv.DictWriter(o_new, fieldnames=outfile_header, delimiter='\t')
                o_new_write.writeheader()

                for sheet_name, sheet_id in sorted(sheet_info.items()):

                    mss_update_col = []
                    updated_rows = []

                    updated_samples = 0
                    already_seq_complete_samples = 0

                    sheet_col_ids = ss_conn.get_column_ids(sheet_id)

                    # get required column id's to pull from sheet
                    for col_title, col_id in sheet_col_ids.items():
                        if col_title in self.mss_required_columns:
                            mss_update_col.append(col_id)

                    mss_sheet = ss_conn.get_sheet_with_columns(sheet_id=sheet_id, column_list=mss_update_col)

                    for row in mss_sheet.rows:

                        sample_found = False
                        swoid_found = False
                        sample_data_dict = dict.fromkeys(outfile_header, 'NA')

                        for cell in row.cells:

                            if cell.column_id == sheet_col_ids['Work Order ID']:
                                swoid = cell.value
                                sample_data_dict['Work Order ID'] = swoid
                                for k, v in sample_data.items():
                                    if str(swoid) in sample_data[k]['WorkOrder']:
                                        swoid_found = True

                            if cell.column_id == sheet_col_ids['Sample Full Name']:
                                sample_name = cell.value
                                sample_data_dict['Sample Full Name'] = sample_name
                                for sample in sample_data.keys():
                                    if sample_name in sample:
                                        sample_found = True

                            if cell.column_id == sheet_col_ids['Current Production Status']:
                                sample_production_status = cell.value
                                sample_data_dict['Current Production Status'] = sample_production_status

                            if cell.column_id == sheet_col_ids['Pipeline']:
                                sample_data_dict['Pipeline'] = cell.value

                            if cell.column_id == sheet_col_ids['Sequencing Completed Date']:
                                sample_data_dict['Sequencing Completed Date'] = cell.value

                        if sample_found and swoid_found:

                            if sample_production_status == 'Sequencing Completed' or 'QC' in sample_production_status:
                                total_existing_seq_complete_samples += 1
                                already_seq_complete_samples += 1
                                seq_complete_samples.append(sample_name)
                                f_write.writerow(sample_data_dict)
                                continue

                            if sample_production_status != 'Sequencing Completed' or 'QC' not in sample_production_status:
                                updated_samples += 1

                                new_row = ss_conn.smart_sheet_client.models.Row()
                                new_row.id = row.id

                                new_cell = ss_conn.smart_sheet_client.models.Cell()
                                new_cell.column_id = sheet_col_ids['Current Production Status']
                                new_cell.value = 'Sequencing Completed'
                                sample_data_dict['Current Production Status'] = 'Sequencing Completed'
                                new_row.cells.append(new_cell)

                                new_cell = ss_conn.smart_sheet_client.models.Cell()
                                new_cell.column_id = sheet_col_ids['Sequencing Completed Date']
                                new_cell.value = datetime.datetime.now().isoformat()
                                sample_data_dict['Sequencing Completed Date'] = datetime.datetime.now().isoformat()
                                new_row.cells.append(new_cell)

                                new_cell = ss_conn.smart_sheet_client.models.Cell()
                                new_cell.column_id = sheet_col_ids['Pipeline']
                                new_cell.value = admin_info['Pipeline']
                                sample_data_dict['Pipeline'] = admin_info['Pipeline']
                                new_row.cells.append(new_cell)

                                new_cell = ss_conn.smart_sheet_client.models.Cell()
                                new_cell.column_id = sheet_col_ids['WOI Status']
                                new_cell.value = admin_info['Status']
                                new_row.cells.append(new_cell)

                                updated_rows.append(new_row)
                                update_sample_name.append(sample_name)
                                f_write.writerow(sample_data_dict)
                                o_new_write.writerow(sample_data_dict)

                                continue

                    update = ss_conn.smart_sheet_client.Sheets.update_rows(mss_sheet.id, updated_rows)
                    print(sheet_name)
                    print('Samples updated: {}'.format(updated_samples))

                    total_samples = total_samples - updated_samples
                    total_updated_samples += updated_samples

            print('\nFailed to update samples:')
            # for sample in sample_data:
            #     if sample not in update_sample_name and sample not in seq_complete_samples:
            #         print(sample)
            # return total_updated_samples

            for sample in sample_data:
                update_found = False
                seq_update = False
                for update_sample in update_sample_name:
                    if update_sample in sample:
                        update_found = True

                for seq_complete_sample in seq_complete_samples:
                    if seq_complete_sample in sample:
                        seq_update = True
                if not update_found and not seq_update:
                    print(sample)

            return total_updated_samples, attachment_files

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

        print('\nUpdating Production Communications Sheet')
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
        # new_swo_row.cells.append({'column_id': col_dict['Facilitator'], 'object_value': admin_info['user email']})
        new_swo_row.cells.append({'column_id': col_dict['Facilitator'], 'object_value': {
                        "objectType": 'MULTI_CONTACT', 'values': [{'email': admin_info['user email'],
                                                                   'name': admin_info['user email']}]}})
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
        for file in attachment:
            file_line_number = sum(1 for line in open(file))
            if file_line_number > 1:
                ss_connector.smart_sheet_client.Attachments.attach_file_to_row(sheet.id, new_title_row_id,
                                                                               (file, open(file, 'rb'),
                                                                                'application/Excel'))
            sleep(5)

        comment = input('\nSequence Complete Comments (Enter to continue without comment):\n')

        if comment:
            ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(sheet.id, new_title_row_id,
                                                                                 ss_connector.smart_sheet_client.models.
                                                                                 Discussion({'comment': ss_connector.
                                                                                            smart_sheet_client.models.
                                                                                            Comment({'text': comment})})
                                                                                 )

        return




