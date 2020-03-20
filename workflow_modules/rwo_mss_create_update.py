class sampleUpdate:

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
                          'Administration Project',
                          'Attempted Coverage',
                          'Facilitator Comment',
                          'Is For CLE?']

    date_columns = ['Sample Received Date',
                    'RWOID Creation Date',
                    'WOID Creation Date',
                    'Resource Assessment Completed Date',
                    'Lib Core Start Date',
                    'Capture drop off date',
                    'qPCR drop off date',
                    'Initial Sequencing Scheduled Date',
                    'Sequencing Scheduled Date',
                    'Sequencing Completed Date',
                    'QC Start',
                    'QC Completed Date',
                    'Data Transfer Hand Off Date',
                    'Data Transfer Completed Date']

    def __init__(self, smartsheet_sheet_obj, sheet_data, admin, ss_connector, new_sheet_name):

        self.sheet_obj = smartsheet_sheet_obj
        self.sheet_data = sheet_data
        self.new_sheet_name = new_sheet_name
        self.admin = admin['Administration Project']
        self.ss_connector = ss_connector
        self.woid = admin['Work Order ID']
        self.pipeline = admin['Pipeline']
        self.description = admin['Description']
        self.woid_date = admin['WO Start Date']
        self.admin_info_dict = {'Creator': admin['Creator'],
                                'WO Facilitator': admin['Facilitator'],
                                'Billing Acct Name': admin['Billing Account'],
                                'Facilitator Comment': admin['Facilitator Comment'],
                                'Is For CLE?': admin['Is For CLE?'],
                                'user email': admin['user email']}

        self.rwo = False
        self.swo = True
        if 'Resource Storage' in self.pipeline:
            self.rwo = True
            self.swo = False

    def construct_sheet(self, sheet_name, *args):

        new_sheet = {'name': sheet_name, 'columns': []}

        new_sheet['columns'].append({'title': 'RWO Unique', 'type': 'CHECKBOX', 'symbol': 'STAR', 'width': 60})
        new_sheet['columns'].append({'title': 'Current Iteration', 'type': 'CHECKBOX', 'symbol': 'STAR', 'width': 60})
        new_sheet['columns'].append({'title': 'Iteration', 'type': 'TEXT_NUMBER', 'width': 60})
        new_sheet['columns'].append({'title': 'Fail', 'type': 'CHECKBOX', 'symbol': 'FLAG', 'width': 5.33})
        new_sheet['columns'].append({'title': 'Re-attempt', 'type': 'PICKLIST', 'symbol': 'DECISION_SYMBOLS',
                                     'width': 60})

        for col in self.new_column_headers:
            if col == 'Sample Full Name':
                new_sheet['columns'].append({'title': col, 'type': 'TEXT_NUMBER', 'primary': True})
            elif col == 'WO Facilitator':
                new_sheet['columns'].append({'title': col, 'type': 'CONTACT_LIST'})
            else:
                new_sheet['columns'].append({'title': col, 'type': 'TEXT_NUMBER'})

        for date_col in self.date_columns:
            new_sheet['columns'].append({'title': date_col, 'type': 'DATE'})

        new_sheet['columns'].append({'title': 'Duration', 'type': 'TEXT_NUMBER'})
        new_sheet['columns'].append({'title': 'Topup', 'type': 'CHECKBOX', 'width': 60})
        new_sheet['columns'].append({'title': 'Launched', 'type': 'CHECKBOX', 'width': 60})
        new_sheet['columns'].append({'title': 'Data Transfer Completed', 'type': 'CHECKBOX', 'width': 60})

        sheet_spec = self.ss_connector.smart_sheet_client.models.Sheet(new_sheet)

        if not args:
            response = self.ss_connector.smart_sheet_client.Folders.create_sheet_in_folder(self.sheet_obj, sheet_spec)
        else:
            response = self.ss_connector.smart_sheet_client.Folders.create_sheet_in_folder(args[0], sheet_spec)
        return response.result.id

    def create_row(self, sample_row_dict, sheet_columns):

        new_row = self.ss_connector.smart_sheet_client.models.Row()

        for header in self.new_column_headers:

            if header == 'Resource Storage' and self.rwo:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': self.woid, 'hyperlink': {
                    'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(self.woid)}})
                continue

            if header == 'Resource Storage' and not self.rwo:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': 'NA'})
                continue

            if header == 'Work Order ID' and self.swo:
                new_row.cells.append(
                    {'column_id': sheet_columns['Work Order ID'], 'value': self.woid, 'hyperlink': {
                     'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(self.woid)}})
                continue

            if header == 'Work Order ID' and not self.swo:
                new_row.cells.append({'column_id': sheet_columns['Work Order ID'], 'value': 'NA'})
                continue

            if header == 'Resource Bank Barcode' and self.rwo:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': sample_row_dict['Barcode']})
                continue

            if header == 'Resource Bank Barcode' and not self.rwo:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': 'NA'})
                continue

            if header == 'Pipeline':
                new_row.cells.append({'column_id': sheet_columns['Pipeline'], 'value': self.pipeline})
                continue

            if header == 'Current Production Status' and self.rwo:
                new_row.cells.append(
                    {'column_id': sheet_columns['Current Production Status'], 'value': 'Resource Storage'})
                continue

            if header == 'Current Production Status' and not self.rwo and not self.swo:
                new_row.cells.append(
                    {'column_id': sheet_columns['Current Production Status'], 'value': self.pipeline})
                continue

            if header == 'Current Production Status' and self.swo:
                new_row.cells.append(
                    {'column_id': sheet_columns['Current Production Status'], 'value': 'Resource Assessment Pass'})
                continue

            if header == 'Administration Project':
                new_row.cells.append({'column_id': sheet_columns['Administration Project'], 'value': self.admin})
                continue

            if header == 'WO Facilitator':
                new_row.cells.append({'column_id': sheet_columns['WO Facilitator'],
                                      'value': self.admin_info_dict['user email']})
                continue

            if header == 'RWOID Description' and self.rwo:
                new_row.cells.append({'column_id': sheet_columns['RWOID Description'], 'value': self.description})
                continue

            if header == 'SWOID Description' and self.swo:
                new_row.cells.append({'column_id': sheet_columns['SWOID Description'], 'value': self.description})
                continue

            # TODO handle two case controls if present
            if header == 'Case Control Status':
                case_found = False
                for k in sample_row_dict.keys():
                    if 'case' in k.lower() or 'disease_status' in k.lower():
                        case_found = True
                        new_row.cells.append({'column_id': sheet_columns['Case Control Status'],
                                              'value': sample_row_dict[k]})
                        break

                if not case_found:
                    new_row.cells.append({'column_id': sheet_columns['Case Control Status'], 'value': 'NA'})
                    continue

                continue

            if header in self.admin_info_dict.keys():
                new_row.cells.append({'column_id': sheet_columns[header], 'value': self.admin_info_dict[header]})
                continue

            if header not in sample_row_dict:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': 'NA'})
            else:
                new_row.cells.append({'column_id': sheet_columns[header], 'value': sample_row_dict[header]})

        if self.rwo:
            new_row.cells.append({'column_id': sheet_columns['RWOID Creation Date'],
                                  'value': self.woid_date})

        if self.swo:
            new_row.cells.append({'column_id': sheet_columns['WOID Creation Date'],
                                  'value': self.woid_date})

        new_row.cells.append({'column_id': sheet_columns['RWO Unique'], 'value': True})
        new_row.cells.append({'column_id': sheet_columns['Current Iteration'], 'value': True})
        new_row.cells.append({'column_id': sheet_columns['Iteration'], 'value': 1})
        new_row.to_bottom = True
        return new_row

    def get_column_ids(self, sheet_column_object):
        column_id_dict = {}
        for col in sheet_column_object:
            column_id_dict[col.title] = col.id
        return column_id_dict

    def dict_slice(self, dct, low=None, high=None):
        return dict(list(sorted(dct.items()))[low:high])

    def write_to_sheet(self):

        sheet = self.sheet_obj[0]
        sheet_col_id_dict = self.sheet_obj[1]

        if len(self.sheet_obj) == 3:
            admin_folder_id = self.sheet_obj[2]

        ss_available_rows_to_write = 1600 - sheet.total_row_count

        # all samples fit on one sheet (3 keys in data are not samples)
        if len(self.sheet_data) < ss_available_rows_to_write:
            rows_to_write_to_smart_sheet = []
            for i, k in enumerate(self.sheet_data.keys()):
                i = self.create_row(self.sheet_data[k], sheet_col_id_dict)
                rows_to_write_to_smart_sheet.append(i)
            response = self.ss_connector.smart_sheet_client.Sheets.add_rows(sheet.id, rows_to_write_to_smart_sheet)
            # print(response.data)
            print('{} samples written to {}'.format(len(rows_to_write_to_smart_sheet), sheet.name))

        else:

            samples_written = 0

            # complete current sheet
            if ss_available_rows_to_write > 0:
                rows_to_complete_sheet_dict = self.dict_slice(self.sheet_data, high=ss_available_rows_to_write)
                rows_to_write_to_new_sheet_dict = self.dict_slice(self.sheet_data, low=ss_available_rows_to_write,
                                                                  high=len(self.sheet_data))
                rows_to_complete_sheet = []
                for i, k in enumerate(rows_to_complete_sheet_dict.keys()):
                    samples_written += 1
                    i = self.create_row(rows_to_complete_sheet_dict[k], sheet_col_id_dict)
                    rows_to_complete_sheet.append(i)
                response = self.ss_connector.smart_sheet_client.Sheets.add_rows(sheet.id, rows_to_complete_sheet)
                # print(response.data)
                print('{} samples written to {}'.format(len(rows_to_complete_sheet), sheet.name))

            else:
                # left over samples left to write
                rows_to_write_to_new_sheet_dict = self.sheet_data.copy()

            sample_counter = len(rows_to_write_to_new_sheet_dict)
            make_sheet = True

            while sample_counter > 0 and make_sheet:

                # make new sheet
                mss_sheets = self.ss_connector.get_sheet_list(admin_folder_id, 'f')
                highest_sheet = 1
                for mss_sheet in mss_sheets:
                    s = mss_sheet.name
                    if 'MSS_' in s:
                        sheet_int = int(s.split('_')[-1])
                        if sheet_int > highest_sheet:
                            highest_sheet = sheet_int

                new_sheet_id = self.construct_sheet('{}{}'.format(self.new_sheet_name[:-1], (highest_sheet + 1)),
                                                    admin_folder_id)
                new_sheet_object = self.ss_connector.get_object(new_sheet_id, 's')
                new_sheet_columns_dict = self.get_column_ids(new_sheet_object.columns)

                if len(rows_to_write_to_new_sheet_dict) <= 1600:
                    make_sheet = False
                    sample_counter = sample_counter - len(rows_to_write_to_new_sheet_dict)
                    rows_to_complete_sheet = []
                    for i, k in enumerate(rows_to_write_to_new_sheet_dict.keys()):
                        samples_written += 1
                        i = self.create_row(rows_to_write_to_new_sheet_dict[k], new_sheet_columns_dict)
                        rows_to_complete_sheet.append(i)
                    response = self.ss_connector.smart_sheet_client.Sheets.add_rows(new_sheet_id,
                                                                                    rows_to_complete_sheet)
                    print('{} samples written to {}'.format(len(rows_to_complete_sheet), new_sheet_object.name))

                else:

                    rows_to_write = self.dict_slice(rows_to_write_to_new_sheet_dict, high=1600)
                    rows_left = self.dict_slice(rows_to_write_to_new_sheet_dict, low=1600,
                                                high=len(rows_to_write_to_new_sheet_dict))

                    rows_to_complete_sheet = []
                    for i, k in enumerate(rows_to_write.keys()):
                        samples_written += 1
                        i = self.create_row(rows_to_write[k], new_sheet_columns_dict)
                        rows_to_complete_sheet.append(i)
                    response = self.ss_connector.smart_sheet_client.Sheets.add_rows(new_sheet_id,
                                                                                    rows_to_complete_sheet)
                    print('{} samples written to {}'.format(len(rows_to_complete_sheet), new_sheet_object.name))
                    sample_counter = sample_counter - len(rows_to_complete_sheet)
                    rows_to_write_to_new_sheet_dict = rows_left.copy()

            print('Total samples added to smartsheet: {}'.format(samples_written))
