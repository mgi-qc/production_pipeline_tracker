import os
import smartsheet
from time import sleep


class phRowUpdate:

    # TODO: Get rid of rows
    ph_rows_to_initialize = ['Resource Storage',
                             'Illumina Whole Genome Sequencing',
                             'IDT Exome Sequencing',
                             'Illumina RNA Sequencing',
                             'Analysis']

    def __init__(self, ph_sheet_object, col_dict, admin_dict, woid):

        self.sheet = ph_sheet_object
        self.col_dict = col_dict
        self.admin = admin_dict
        self.woid = woid

    def duplicate_rwo_check(self, type_):

        if type_ == 'ph':
            key_ = 'Projects'
        if type_ == 'phc':
            key_ = 'Reporting Instance'

        for row in self.sheet.rows:
            for cell in row.cells:
                if cell.column_id == self.col_dict[key_] and self.woid in str(cell.value):
                    return True
        return False

    def write_row(self, ss_connector, row_id):

        attachment = '{}.sample.tsv'.format(self.woid)

        new_row = ss_connector.smart_sheet_client.models.Row()
        if self.admin['Status'] == 'abandoned':
            new_row.cells.append(
                {'column_id': self.col_dict['Projects'], 'value': self.woid, 'hyperlink': {
                    'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(self.woid)},
                 "format": ",,,,,,,,,27,,,,,,"})
        else:
            new_row.cells.append(
                {'column_id': self.col_dict['Projects'], 'value': self.woid, 'hyperlink': {
                    'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(self.woid)}})
        new_row.cells.append({'column_id': self.col_dict['Items'], 'value': 0})
        # new_row.cells.append(
        #     {'column_id': self.col_dict['Administration Project'], 'value': self.admin['Administration Project']})
        new_row.cells.append({'column_id': self.col_dict['Pipeline'], 'value': self.admin['Pipeline']})
        new_row.cells.append({'column_id': self.col_dict['Description'], 'value': self.admin['Description']})
        new_row.cells.append({'column_id': self.col_dict['Date Created'], 'value': self.admin['WO Start Date']})
        new_row.cells.append({'column_id': self.col_dict['WOID Status'], 'value': self.admin['Status']})
        new_row.cells.append({'column_id': self.col_dict['Facilitator'], 'object_value': self.admin['user email']})
        new_row.cells.append({'column_id': self.col_dict['Billing Account'], 'value': self.admin['Billing Account']})
        new_row.cells.append({'column_id': self.col_dict['Accounts Payable Contact'], 'value': 'NHGRI'})

        new_row.to_bottom = True
        new_row.parent_id = row_id

        print('Appending {} to {}:\n{}'.format(self.woid, self.admin['Administration Project'], self.admin['Pipeline']))

        # response = ss_connector.smart_sheet_client.Sheets.add_rows(self.sheet.id, [new_row]).data
        # Make new connection to get updated workspace, prevent 'Parent row id not found error'
        new_ss_connector = smartsheet.Smartsheet(os.environ.get('SMRT_API'))
        response = new_ss_connector.Sheets.add_rows(self.sheet.id, [new_row]).data

        for r in response:
            new_row_id = r.id

        if os.path.isfile(attachment):
            sleep(2)
            # ss_connector.smart_sheet_client.Attachments.attach_file_to_row(
            #     self.sheet.id, new_row_id, (attachment, open(attachment, 'rb'), 'application/Excel'))
            new_ss_connector.Attachments.attach_file_to_row(self.sheet.id, new_row_id,
                                                            (attachment, open(attachment, 'rb'), 'application/Excel'))
        return

    def ph_update(self, ss_connector, update=None):

        admin_found = False
        pipeline_row_found = False
        for row in self.sheet.rows:
            for cell in row.cells:
                # get row id for parent project
                if cell.column_id == self.col_dict['Projects'] and cell.value == 'Project Name':
                    project_name_row_id = row.id

                if cell.column_id == self.col_dict['Projects'] and self.admin['Administration Project'] == cell.value:
                    print(
                        '\nAdmin project {} found in Project Hub.'.format(self.admin['Administration Project']))
                    admin_found = True
                    admin_row_number = row.id

                # if admin_found and rwo_row_number == row.row_number and cell.value == 'Resource Storage':
                if admin_found and cell.value == self.admin['Pipeline']:
                    self.write_row(ss_connector, row.id)
                    return True

        if admin_found and not pipeline_row_found:
            new_rwo_header_row = ss_connector.smart_sheet_client.models.Row({"format": ",,1,,,,,,,18,,,,,,"})
            new_rwo_header_row.cells.append({'column_id': self.col_dict['Projects'], 'value': self.admin['Pipeline']})
            new_rwo_header_row.cells.append({'column_id': self.col_dict['Items'], 'formula': '=SUM(CHILDREN())'})
            new_rwo_header_row.cells.append({'column_id': self.col_dict['WOID Status'],
                                             'formula': '=SUM(CHILDREN())'})
            new_rwo_header_row.to_bottom = True
            new_rwo_header_row.parent_id = admin_row_number

            rwo_row_response = ss_connector.smart_sheet_client.Sheets.add_rows(self.sheet.id, [new_rwo_header_row])

            for r in rwo_row_response.data:
                new_rwo_header_row_number = r.id

            # use write_row function to populate rwo row with fields and write
            attempts = 0
            while attempts < 3:
                try:
                    self.write_row(ss_connector, new_rwo_header_row_number)
                except ss_connector.exceptions.SmartsheetException as e:
                    if isinstance(e, ss_connector.exceptions.ApiError):
                        print(e.error.result.error_code)
                        print(e.error.result.message)
                        print(e.error.result.name)
                        print(e.error.result.recommendation)
                else:
                    break

            sleep(5)
            return True

        if not admin_found and update:
            return False

        if not admin_found:

            print('\nAdmin project {} does not exist in Project Hub.\nCreating new project.'.format(
                self.admin['Administration Project']))
            # create admin row
            new_admin_row = ss_connector.smart_sheet_client.models.Row({"format": ",,1,,,,,,,15,,,,,,"})
            new_admin_row.cells.append(
                {'column_id': self.col_dict['Projects'], 'value': self.admin['Administration Project'], 'hyperlink': {
                    'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'.format(
                        self.admin['Administration Project'].replace(' ', '+'))}})
            new_admin_row.cells.append({'column_id': self.col_dict['Items'], 'formula': '=SUM(CHILDREN())'})
            new_admin_row.cells.append({'column_id': self.col_dict['WOID Status'],
                                        'formula': '=SUM(CHILDREN())'})
            new_admin_row.cells.append({'column_id': self.col_dict['Facilitator'],
                                        'object_value': self.admin['user email']})
            new_admin_row.to_bottom = True
            new_admin_row.parent_id = project_name_row_id

            admin_row_response = ss_connector.smart_sheet_client.Sheets.add_rows(self.sheet.id, [new_admin_row])

            for r in admin_row_response.data:
                new_admin_row_number = r.id

            # create other title rows
            pipeline_added = False
            if self.admin['Pipeline'] not in self.ph_rows_to_initialize:
                self.ph_rows_to_initialize.append(self.admin['Pipeline'])

            title_row_add_list = []
            for title_row in self.ph_rows_to_initialize:
                new_header_row = ss_connector.smart_sheet_client.models.Row({"format": ",,1,,,,,,,18,,,,,,"})
                new_header_row.cells.append({'column_id': self.col_dict['Projects'], 'value': title_row})
                new_header_row.cells.append({'column_id': self.col_dict['Items'], 'formula': '=SUM(CHILDREN())'})
                new_header_row.cells.append({'column_id': self.col_dict['WOID Status'],
                                             'formula': '=SUM(CHILDREN())'})
                new_header_row.to_bottom = True
                new_header_row.parent_id = new_admin_row_number

                title_row_add_list.append(new_header_row)

            attempts = 0
            while attempts < 3:
                try:
                    response = ss_connector.smart_sheet_client.Sheets.add_rows(self.sheet.id, title_row_add_list)
                    attempts += 1
                except ss_connector.exceptions.SmartsheetException as e:
                    if isinstance(e, ss_connector.exceptions.ApiError):
                        print(e.error.result.error_code)
                        print(e.error.result.message)
                        print(e.error.result.name)
                        print(e.error.result.recommendation)

                else:
                    break

            for r in response.data:
                for cell in r.cells:
                    if cell.value == self.admin['Pipeline']:
                        return self.write_row(ss_connector, r.id)

    def pcc_update(self, ss_connector):

        print('Adding {} to Production Communications Sheet'.format(self.woid))
        for row in self.sheet.rows:
            for cell in row.cells:
                if cell.column_id == self.col_dict['Reporting Instance'] and cell.value == \
                        'Smartsheet Work Order Initiation':

                    new_rba_row = ss_connector.smart_sheet_client.models.Row({"format": ",,,,,,,,,18,,,,,,"})
                    new_rba_row.cells.append({'column_id': self.col_dict['Reporting Instance'], 'value': self.woid,
                                              'hyperlink':
                                             {'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'
                                             .format(self.woid)}})

                    new_rba_row.cells.append(
                        {'column_id': self.col_dict['Admin Project'], 'value': self.admin['Administration Project'],
                         'hyperlink': {
                             'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'
                            .format(self.admin['Administration Project'].replace(' ', '+'))}})

                    # new_rba_row.cells.append({'column_id': self.col_dict['Facilitator'],
                    #                           'object_value': self.admin['user email']})
                    new_rba_row.cells.append({'column_id': self.col_dict['Facilitator'], 'object_value': {
                        "objectType": 'MULTI_CONTACT', 'values': [{'email': self.admin['user email'],
                                                                   'name': self.admin['user email']}]}})
                    new_rba_row.cells.append(
                        {'column_id': self.col_dict['Event Date'], 'value': self.admin['WO Start Date']})
                    new_rba_row.cells.append(
                        {'column_id': self.col_dict['Production Notes'], 'value': 'Smartflow Initiation'})
                    new_rba_row.cells.append({'column_id': self.col_dict['Items'], 'value': 0})
                    new_rba_row.to_bottom = True
                    new_rba_row.parent_id = row.id

                    response = ss_connector.smart_sheet_client.Sheets.add_rows(self.sheet.id, [new_rba_row])

                    for r in response.data:
                        new_row_id = r.id

                    attachment = '{}.sample.tsv'.format(self.woid)

                    if os.path.isfile(attachment):
                        sleep(2)
                        ss_connector.smart_sheet_client.Attachments.attach_file_to_row(
                            self.sheet.id, new_row_id, (attachment, open(attachment, 'rb'), 'application/Excel'))
                        sleep(10)

    def update_sample_number(self, ss_connector, sample_num, type_=None):

        str(sample_num)

        col = 'Projects'
        if type_ == 'pcc':
            col = 'Reporting Instance'

        for row in self.sheet.rows:
            for cell in row.cells:
                # get row id for parent project
                if cell.column_id == self.col_dict[col] and cell.value == self.woid:

                    new_row = ss_connector.smart_sheet_client.models.Row()
                    new_row.id = row.id

                    new_cell = ss_connector.smart_sheet_client.models.Cell()
                    new_cell.column_id = self.col_dict['Items']
                    new_cell.value = sample_num
                    new_row.cells.append(new_cell)

                    update = ss_connector.smart_sheet_client.Sheets.update_rows(self.sheet.id, [new_row])
                    return update

    def ph_mss_url_add_update(self, ss_conn, admin, ph_sheet_col_dict, mss_sheets):

        # get sheet urls
        sheet_urls = {}
        comment = 'This row comment section for MSS sheet url\'s only\n'
        for folder_id, sheets in mss_sheets.items():
            for sheet_name, sheet_id in sorted(sheets.items()):
                sheet = ss_conn.smart_sheet_client.Sheets.get_sheet(sheet_id)
                sheet_urls[sheet_name] = sheet.permalink
                comment += '{}: {}\n'.format(sheet_name, sheet.permalink)

        ph_sheet, ph_col_dict = ph_sheet_col_dict

        for row in ph_sheet.rows:
            for cell in row.cells:
                if cell.column_id == ph_col_dict['Projects'] and cell.value == admin:
                    response = ss_conn.smart_sheet_client.Discussions.get_row_discussions(ph_sheet.id, row.id,
                                                                                          include_all=True)
                    row_comment = response.data
                    if not row_comment:
                        ss_conn.smart_sheet_client.Discussions.create_discussion_on_row(
                            ph_sheet.id, row.id, ss_conn.smart_sheet_client.models.Discussion(
                                {'comment':  ss_conn.smart_sheet_client.models.Comment({'text': comment})}))

                    if row_comment:
                        for c in row_comment:
                            ss_conn.smart_sheet_client.Discussions.delete_discussion(ph_sheet.id, c.id)
                        ss_conn.smart_sheet_client.Discussions.create_discussion_on_row(
                            ph_sheet.id, row.id, ss_conn.smart_sheet_client.models.Discussion(
                                {'comment': ss_conn.smart_sheet_client.models.Comment({'text': comment})}))
        return
