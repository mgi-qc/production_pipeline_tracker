class SampleUpdate:

    new_column_headers = ['Sample Full Name',
                          'Resource Storage',
                          'Work Order ID',
                          'Pipeline',
                          'Current Production Status',
                          'QC Failed Metrics',
                          'WOI Status',
                          'Creation Date',
                          'Sample Name',
                          'Sample Type',
                          'Resource Bank Barcode',
                          'Tissue Name',
                          'Sample Nomenclature',
                          'RWOID Description',
                          'WOID Description',
                          'Case Control Status',
                          'Creator',
                          'WO Facilitator',
                          'Billing Acct Name',
                          'Administration Project'
                          'Attempted Coverage',
                          'Facilitator Comment',
                          'Is For CLE?']

    date_columns = ['Sample Received Date',
                    'RWOID Creation Date',
                    'WOID Creation Date',
                    'Resource Assessment Completed Date',
                    'Lib Core Start Date',
                    'New Library Needed',
                    'Capture drop off date',
                    'qPCR drop off date',
                    'Initial Sequencing Scheduled Date',
                    'Sequencing Scheduled Date',
                    'Sequencing Completed Date',
                    'QC Start',
                    'QC Completed Date',
                    'Data Transfer Hand Off Date',
                    'Data Transfer Completed Date']

    def __init__(self, ss_conn=None, sheet_dict=None, samples=None, project_info=None, date=None):

        self.ss_connector = ss_conn
        self.sheets_in_folder_dict = sheet_dict
        self.samples = samples
        self.project_info = project_info
        self.date = date
        status = self.project_info['Pipeline']
        if 'sequencing' in self.project_info['Pipeline'].lower():
            status = 'Resource Assessment Pass'
        self.field_values = {'Work Order ID': self.project_info['Work Order ID'],
                             'Current Production Status': status,
                             'Pipeline': self.project_info['Pipeline'],
                             'WOI Status': self.project_info['Status'],
                             'WOID Creation Date': project_info['WO Start Date'],
                             'WOID Description': project_info['Description']}

    def get_column_ids(self, sheet_column_id_object):
        column_id_dict = {}
        for col in sheet_column_id_object:
            column_id_dict[col.title] = col.id
        return column_id_dict

    def construct_sheet(self, sheet_name, folder_id):

        new_sheet = {'name': sheet_name, 'columns': []}

        new_sheet['columns'].append({'title': 'RWO Unique', 'type': 'CHECKBOX', 'symbol': 'STAR', 'width': 60})
        new_sheet['columns'].append({'title': 'Current Iteration', 'type': 'CHECKBOX', 'symbol': 'STAR', 'width': 60})
        new_sheet['columns'].append({'title': 'Iteration', 'type': 'TEXT_NUMBER', 'width': 60})
        new_sheet['columns'].append({'title': 'Fail', 'type': 'CHECKBOX', 'symbol': 'FLAG', 'width': 5.33})
        new_sheet['columns'].append({'title': 'Re-attempt', 'type': 'PICKLIST', 'symbol': 'DECISION_SYMBOLS',
                                     'width': 60})
        new_sheet['columns'].append({'title': 'Aliquot Requested', 'type': 'CHECKBOX', 'width': 70})

        for col in self.new_column_headers:
            if col == 'Sample Full Name':
                new_sheet['columns'].append({'title': col, 'type': 'TEXT_NUMBER', 'primary': True})
            else:
                new_sheet['columns'].append({'title': col, 'type': 'TEXT_NUMBER'})

        for date_col in self.date_columns:
            new_sheet['columns'].append({'title': date_col, 'type': 'DATE'})

        new_sheet['columns'].append({'title': 'Duration', 'type': 'TEXT_NUMBER'})
        new_sheet['columns'].append({'title': 'Topup', 'type': 'CHECKBOX', 'width': 60})
        new_sheet['columns'].append({'title': 'Launched', 'type': 'CHECKBOX', 'width': 60})
        new_sheet['columns'].append({'title': 'Data Transfer Completed', 'type': 'CHECKBOX', 'width': 60})

        sheet_spec = self.ss_connector.smart_sheet_client.models.Sheet(new_sheet)

        response = self.ss_connector.smart_sheet_client.Folders.create_sheet_in_folder(folder_id, sheet_spec)

        return response.result.id

    def get_child_rows(self, sheet_id):

        columns = ['Iteration', 'Sample Full Name']
        mss_update_col = []
        sample_iteration_number = {}

        sheet_col_ids = self.ss_connector.get_column_ids(sheet_id)

        # get required column id's to pull from sheet
        for col_title, col_id in sheet_col_ids.items():
            if col_title in columns:
                mss_update_col.append(col_id)

        mss_sheet = self.ss_connector.get_sheet_with_columns(sheet_id=sheet_id, column_list=mss_update_col)

        for row in mss_sheet.rows:

            if not row.parent_id:
                continue

            for cell in row.cells:
                if cell.column_id == sheet_col_ids['Sample Full Name']:
                    sample_name = cell.value

                if cell.column_id == sheet_col_ids['Iteration']:
                    iteration_number = cell.value

            if sample_name not in sample_iteration_number:
                sample_iteration_number[sample_name] = {}
                sample_iteration_number[sample_name]['iteration'] = iteration_number
                sample_iteration_number[sample_name]['row_ids'] = [row.id]
                sample_iteration_number[sample_name]['update'] = False
            else:
                sample_iteration_number[sample_name]['iteration'] = iteration_number
                sample_iteration_number[sample_name]['row_ids'].append(row.id)

        return sample_iteration_number

    def update_sample(self):

        for folder, sheet_info in self.sheets_in_folder_dict.items():

            sample_woid_exists = []

            # for each sheet in sheet_dict
            for sheet_name, sheet_id in sorted(sheet_info.items()):

                updated_rows = []
                new_rows = []
                update_existing_rows = []
                duplicate_samples_added = 0

                # update sheet
                sheet = self.ss_connector.get_object(sheet_id, 's')
                # update sheet column id's
                sheet_column_ids = self.get_column_ids(sheet.columns)

                sample_iteration_number = self.get_child_rows(sheet_id)

                # for each row in sheet, if sample in samples
                for row in sheet.rows:

                    # skip row if it has a parent id (row is child row)
                    if row.parent_id:
                        continue

                    sample_match = False
                    woid_match = False

                    update_existing_row = self.ss_connector.smart_sheet_client.models.Row()
                    update_existing_row.id = row.id

                    new_sibling_row = self.ss_connector.smart_sheet_client.models.Row()
                    new_sibling_row.parent_id = row.id
                    new_sibling_row.to_bottom = True

                    for cell in row.cells:

                        if cell.column_id == sheet_column_ids['Sample Full Name']:

                            sample_name = cell.value

                            # change color of existing sample name to show it's duplicate
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell({"format": ",,,,,,,,,23,,,,,,"})
                            new_cell.column_id = cell.column_id
                            new_cell.value = cell.value
                            update_existing_row.cells.append(new_cell)

                            append_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            append_cell.column_id = cell.column_id
                            append_cell.value = cell.value
                            new_sibling_row.cells.append(append_cell)

                            if sample_name in self.samples:
                                sample_match = True

                            continue

                        if cell.column_id == sheet_column_ids['Work Order ID']:

                            row_woid = cell.value
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = cell.column_id
                            new_cell.value = cell.value
                            new_cell.hyperlink = {
                                'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(row_woid)}
                            update_existing_row.cells.append(new_cell)

                            append_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            append_cell.column_id = cell.column_id
                            append_cell.value = self.project_info['Work Order ID']
                            append_cell.hyperlink = {
                                'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(
                                    self.project_info['Work Order ID'])}
                            new_sibling_row.cells.append(append_cell)

                            if row_woid == self.field_values['Work Order ID'] or row_woid == 'NA':
                                woid_match = True

                            continue

                        if cell.column_id in [sheet_column_ids['Pipeline'],
                                              sheet_column_ids['Current Production Status']]:

                            key = list(sheet_column_ids.keys())[list(sheet_column_ids.values()).index(cell.column_id)]
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids[key]
                            new_cell.value = self.field_values[key]
                            new_sibling_row.cells.append(new_cell)

                            continue

                        if cell.column_id == sheet_column_ids['WOID Creation Date']:
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids['WOID Creation Date']
                            new_cell.value = self.project_info['WO Start Date']
                            new_sibling_row.cells.append(new_cell)
                            continue

                        if cell.column_id == sheet_column_ids['WOID Description']:
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids['WOID Description']
                            new_cell.value = self.project_info['Description']
                            new_sibling_row.cells.append(new_cell)
                            continue

                        if cell.column_id == sheet_column_ids['Resource Storage']:
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids['Resource Storage']
                            new_cell.value = cell.value
                            new_cell.hyperlink = {
                                'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(cell.value)}
                            new_sibling_row.cells.append(new_cell)
                            continue

                        if cell.column_id == sheet_column_ids['RWO Unique']:
                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids['RWO Unique']
                            new_cell.value = False
                            new_sibling_row.cells.append(new_cell)
                            continue

                        if cell.column_id == sheet_column_ids['Current Iteration']:

                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = cell.column_id
                            new_cell.value = False
                            update_existing_row.cells.append(new_cell)

                            append_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            append_cell.column_id = cell.column_id
                            append_cell.value = True
                            new_sibling_row.cells.append(append_cell)

                            continue

                        if cell.column_id == sheet_column_ids['Iteration']:
                            iteration_value = cell.value
                            continue

                        if cell.value is None:
                            continue

                        key = list(sheet_column_ids.keys())[list(sheet_column_ids.values()).index(cell.column_id)]
                        # print('key:', key)
                        new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                        new_cell.column_id = sheet_column_ids[key]
                        new_cell.value = cell.value
                        new_sibling_row.cells.append(new_cell)

                    if sample_name in sample_iteration_number:
                        new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                        new_cell.column_id = sheet_column_ids['Iteration']
                        new_cell.value = sample_iteration_number[sample_name]['iteration'] + 1
                        new_sibling_row.cells.append(new_cell)
                    else:
                        new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                        new_cell.column_id = sheet_column_ids['Iteration']
                        new_cell.value = iteration_value + 1
                        new_sibling_row.cells.append(new_cell)

                    if sample_match and woid_match:

                        new_row = self.ss_connector.smart_sheet_client.models.Row()
                        new_row.id = row.id

                        for field, value in self.field_values.items():

                            if field == 'Work Order ID':
                                new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                                new_cell.column_id = sheet_column_ids[field]
                                new_cell.value = value
                                new_cell.hyperlink = {
                                    'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(value)}
                                new_row.cells.append(new_cell)
                                continue

                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids[field]
                            new_cell.value = value
                            new_row.cells.append(new_cell)

                        updated_rows.append(new_row)
                        sample_woid_exists.append(sample_name)

                    if sample_match and not woid_match and (sample_name not in sample_woid_exists):
                        sample_woid_exists.append(sample_name)
                        new_rows.append(new_sibling_row)
                        update_existing_rows.append(update_existing_row)
                        if sample_name in sample_iteration_number:
                            sample_iteration_number[sample_name]['update'] = True
                        update = self.ss_connector.smart_sheet_client.Sheets.add_rows(sheet_id, [new_sibling_row])
                        duplicate_samples_added += 1

                if len(updated_rows) > 0:
                    update = self.ss_connector.smart_sheet_client.Sheets.update_rows(sheet.id, updated_rows)

                if len(new_rows) > 0:
                    # update = self.ss_connector.smart_sheet_client.Sheets.add_rows(sheet_id, new_rows)
                    update = self.ss_connector.smart_sheet_client.Sheets.update_rows(sheet.id, update_existing_rows)

                turn_off_iteration_rows = []
                for sample in sample_iteration_number:

                    if sample_iteration_number[sample]['update']:
                        for row_id in sample_iteration_number[sample]['row_ids']:
                            update_row = self.ss_connector.smart_sheet_client.models.Row()
                            update_row.id = row_id

                            new_cell = self.ss_connector.smart_sheet_client.models.Cell()
                            new_cell.column_id = sheet_column_ids['Current Iteration']
                            new_cell.value = False

                            update_row.cells.append(new_cell)

                            turn_off_iteration_rows.append(update_row)

                if len(turn_off_iteration_rows) > 0:
                    update = self.ss_connector.smart_sheet_client.Sheets.update_rows(sheet.id, turn_off_iteration_rows)

                print('{} updated {} existing samples'.format(sheet_name, len(updated_rows)))
                print('{} added {} samples\n'.format(sheet_name, duplicate_samples_added))
