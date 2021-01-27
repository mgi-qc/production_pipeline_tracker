#!/usr/bin/python3.5
import sys
import os
import csv
import argparse
from multiprocessing import Pool
import glob
import datetime
import smartsheet


def ss_connect(conn=None):

    # SS connector
    ss_c = smartsheet.Smartsheet(os.environ.get('SMRT_API'))
    ss_c.errors_as_exceptions()

    if conn:
        return ss_c
    # Get Sequence Complete Sheet Column ID's
    id_dict = get_column_ids(sequence_review_sheet, ss_c)

    # Get Sequence Complete Sheet
    sheet = ss_c.Sheets.get_sheet(sheet_id=sequence_review_sheet)

    return ss_c, sheet, id_dict


def get_email_column_ids(sheet_column_object):

    column_id_dict = {}
    for col in sheet_column_object:
        column_id_dict[col.title] = col.id

    return column_id_dict


def get_column_ids(id_, s_con):

    sheet_col_ids = {}
    for col in s_con.Sheets.get_columns(id_).data:
        sheet_col_ids[col.title] = col.id

    return sheet_col_ids


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


def illumina_info_data(illumina_file, cutoff, billing=None):

    data = {}
    wo_commands = set()

    with open(illumina_file, 'r') as illfile, open('tmp.tsv', 'w') as o:
        for line in illfile:
            if line and '--' not in line and 'Library Index Summary' not in line:
                o.write(line)

    with open('tmp.tsv', 'r') as illumina:
        i_reader = csv.DictReader(illumina, delimiter='\t')
        for l in i_reader:

            sample = l['Library']
            l['Sample Full Name'] = sample.split('-lib')[0]

            if float(l['Total Bases Kb (PF)'].replace(',', '')) >= cutoff:
                l['Seq Complete Status'] = False
                l['Sequence Completed'] = True
            else:
                l['Seq Complete Status'] = True
                l['Sequence Completed'] = False

            if billing:
                wo_commands.add('wo_info --report billing --woid {} --format tsv --output-file-name {}'
                                .format(l['WorkOrder'], '.'.join([l['WorkOrder'], 'wo_info.billing'])))

            # add zero if no value found
            [l.update({x: '0'}) for x, y in l.items() if not y and x not in ['Seq Complete Status',
                                                                             'Sequence Completed']]
            data[sample] = l

    os.remove(illumina_file)
    os.remove('tmp.tsv')
    if billing:
        return data, wo_commands

    return data


def woid_billing_data(billing_file):

    admin_query_dict = {}
    with open(billing_file, 'r') as bf:

        bf_reader = csv.reader(bf, delimiter='\t')

        for line in bf_reader:

            if len(line) == 2 and line[1]:
                field = line[0].replace('"', '').split(':')[0]
                admin_query_dict[field] = line[1].replace('"', '')

        admin_query_dict['Facilitator'] = admin_query_dict['Creator']
        os.remove(billing_file)
    return admin_query_dict


def illumina_info_woid(woid, cutoff):

    illumina_outfile = '.'.join([woid, 'illumina_info_report'])
    woid_billing_outfile = '.'.join([woid, 'wo_info.billing'])

    illumina_info_query_woid = "illumina_info -report library_index_summary --format tsv --woid {} --incomplete " \
                               "--output-file-name {}".format(woid, illumina_outfile)

    illumina_info_email = "illumina_info -report library_index_summary --format email --woid {} --incomplete"\
        .format(woid)

    woid_billing_query = "wo_info --report billing --woid {} --format tsv --output-file-name {} " \
        .format(woid, woid_billing_outfile)

    print('Running illumina_info and billing queries for {}.'.format(woid))
    processes = (illumina_info_query_woid, illumina_info_email, woid_billing_query)
    pool = Pool(processes=3)
    pool.map(os.system, processes)

    for f in [illumina_outfile + '.tsv', woid_billing_outfile + '.tsv']:
        if f not in os.listdir(os.getcwd()):
            sys.exit('{} file failed to create, exiting'.format(f))

    illumina_data = illumina_info_data('.'.join([illumina_outfile, 'tsv']), cutoff)

    woid_info_dict = woid_billing_data('.'.join([woid_billing_outfile, 'tsv']))

    ss_connector, seq_complete_sheet, column_id_dict = ss_connect()

    print('\n{} Update:\n'.format(seq_complete_sheet.name))

    woid_header = False
    woid_exists = False
    updated_rows = []
    updated_samples = []
    new_rows = []

    for row in seq_complete_sheet.rows:

        sample_found = False
        woid_found = False
        complete_status = False

        for cell in row.cells:

            if cell.column_id == column_id_dict['Work Order ID']:
                if cell.value == 'Work Orders':
                    work_orders_parent_row_id = row.id
                    woid_header = True
                    continue

            if woid_header:
                if cell.column_id == column_id_dict['Work Order ID']:
                    if cell.value == woid:
                        woid_found = True
                        if not woid_exists:
                            woid_parent_existing_row = row.id
                            woid_exists = True

            if cell.column_id == column_id_dict['Library']:
                if cell.value is None:
                    continue
                ss_sample = cell.value
                sample_found = True

            if cell.column_id == column_id_dict['Sequence Completed']:
                complete_status = cell.value

        if woid_header and sample_found and woid_found and ss_sample in illumina_data.keys():
            updated_rows.append(update_sibling_row(row.id, illumina_data[ss_sample], ss_connector, column_id_dict,
                                                   complete_status))
            updated_samples.append(ss_sample)

    parent_woid_row = ''

    if woid_exists:

        add_samples = [x for x in illumina_data.keys() if x not in updated_samples]

        parent_woid_row = woid_parent_existing_row

        if len(add_samples) > 0:
            for s in add_samples:
                new_rows.append(
                    create_sibling_row(woid_parent_existing_row, illumina_data[s], woid_info_dict, ss_connector,
                                       column_id_dict))

        if len(updated_rows) > 0:
            print('Updated {} existing samples.'.format(len(updated_rows)))
            ss_connector.Sheets.update_rows(sequence_review_sheet, updated_rows)

        if len(new_rows) > 0:
            print('Added {} new samples'.format(len(new_rows)))
            ss_connector.Sheets.add_rows(sequence_review_sheet, new_rows)

    if not woid_exists:

        print('{} not found, adding to Sequencing Completed Sheet'.format(woid))
        new_woid_parent = create_parent_row(woid_info_dict, ss_connector, column_id_dict, woid,
                                            work_orders_parent_row_id)

        parent_woid_row = new_woid_parent
        sibling_rows = []
        for sample in sorted(illumina_data):
            sibling_rows.append(
                create_sibling_row(new_woid_parent, illumina_data[sample], woid_info_dict, ss_connector,
                                   column_id_dict))

        print('Adding {} Samples.'.format(len(sibling_rows)))
        ss_connector.Sheets.add_rows(sequence_review_sheet, sibling_rows)

    print()
    sample_status_data, library_data, total_samples = update_mss_sheets_woid(illumina_data, woid_info_dict, woid)

    pcc_update(illumina_data,  woid_info_dict, sample_status_data)

    update_woid_row = ss_connector.models.Row()
    update_woid_row.id = parent_woid_row

    a_status = 0
    if 'Abandoned' in sample_status_data:
        a_status = sample_status_data['Abandoned']
    update_woid_row.cells.append({'column_id': column_id_dict['Abandoned'], 'value': a_status})

    nl_status = 0
    if 'New Library Needed' in sample_status_data:
        nl_status = sample_status_data['New Library Needed']
    update_woid_row.cells.append({'column_id': column_id_dict['New Library Needed'], 'value': nl_status})

    qpcr_status = 0
    if 'qPCR drop-off' in sample_status_data:
        qpcr_status = sample_status_data['qPCR drop-off']
    update_woid_row.cells.append(
        {'column_id': column_id_dict['qPCR drop-off'], 'value': qpcr_status})

    i_scheduled = 0
    if 'Initial Sequencing Scheduled' in sample_status_data:
        i_scheduled = sample_status_data['Initial Sequencing Scheduled']
    update_woid_row.cells.append(
        {'column_id': column_id_dict['Initial Sequencing Scheduled'], 'value': i_scheduled})

    s_scheduled = 0
    if 'Sequencing Scheduled' in sample_status_data:
        s_scheduled = sample_status_data['Sequencing Scheduled']
    update_woid_row.cells.append(
        {'column_id': column_id_dict['Sequencing Scheduled'], 'value': s_scheduled})

    update_woid_row.cells.append({'column_id': column_id_dict['Total Samples'], 'value': total_samples})

    ss_connector.Sheets.update_rows(sequence_review_sheet, [update_woid_row])

    # new_lib_status(parent_woid_row, library_data, woid)


def illumina_info_sample(samples, cutoff):

    new_sample_rows = []

    if os.path.isfile('library_index_summary.tsv'):
        os.remove('library_index_summary.tsv')

    info_query_sample = "illumina_info -report library_index_summary --format tsv --sample {} --incomplete"\
        .format(samples)

    info_query_email = "illumina_info -report library_index_summary --format email --sample {} --incomplete"\
        .format(samples)

    print('Running illumina_info and billing queries for:\n{}'.format(samples))
    info_process = (info_query_sample, info_query_email)
    pool = Pool(processes=2)
    pool.map(os.system, info_process)

    if not os.path.isfile('library_index_summary.tsv'):
        sys.exit('{} file not found, illumina query failed, exiting.'.format('library_index_summary.tsv'))

    ill_data, woid_processes = illumina_info_data('library_index_summary.tsv', cutoff, billing=True)

    pool = Pool(processes=20)
    pool.map(os.system, woid_processes)

    wo_info_files = glob.glob('*.wo_info.billing.tsv')
    wo_info_dict = {}

    ss_con, seq_sheet, ids_dict = ss_connect()

    print('\nUpdating {}:'.format(seq_sheet.name))

    for info_file in wo_info_files:
        wo_info_dict[info_file.split('.')[0]] = woid_billing_data(info_file)

    for row in seq_sheet.rows:

        for cell in row.cells:
            if cell.column_id == ids_dict['Work Order ID']:
                if cell.value == 'Illumina Sample Query':
                    samples_parent_row_id = row.id

    for sample in sorted(ill_data):
        new_sample_rows.append(create_sibling_row(samples_parent_row_id, ill_data[sample],
                                                  wo_info_dict[ill_data[sample]['WorkOrder']], ss_con, ids_dict))

    print('Appending {} samples to Ilummina Sample Query row'.format(len(new_sample_rows)))
    ss_con.Sheets.add_rows(sequence_review_sheet, new_sample_rows)
    update_mss_sheets_sample(ill_data, wo_info_dict)


def create_parent_row(billing, sc, column_ids_dict, woid, pr):

    ss_connector = ss_connect(conn='c')

    facilitator = update_admin_email(billing['Facilitator'], sc)
    f_values = [{'email': facilitator, 'name': facilitator}]

    new_woid_row = sc.models.Row({"format": ",,1,,,,,,,15,,,,,,"})

    new_woid_row.cells.append({'column_id': column_ids_dict['Work Order ID'], 'value': woid, 'hyperlink': {
                     'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(woid)}})

    new_woid_row.cells.append(
        {'column_id': column_ids_dict['Admin Project'], 'value': billing['Administration Project'], 'hyperlink': {
            'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'.format(
                billing['Administration Project'].replace(' ', '+'))}})

    new_woid_row.cells.append({'column_id': column_ids_dict['Library'], 'formula': '=COUNT(CHILDREN())'})

    new_woid_row.cells.append({'column_id': column_ids_dict['Sample Name'],
                               'formula': '=COUNT(DISTINCT(COLLECT(CHILDREN(), '
                                          'CHILDREN([Sequence Completed]@row), 1)))'})

    new_woid_row.cells.append({'column_id': column_ids_dict['Facilitator'],
                               'objectValue': {'objectType': 'MULTI_CONTACT', 'values': f_values}})

    new_woid_row.cells.append({'column_id': column_ids_dict['Work Order Description'], 'value': billing['Description']})

    new_woid_row.cells.append({'column_id': column_ids_dict['Pipeline'], 'value': billing['Pipeline']})

    new_woid_row.cells.append({'column_id': column_ids_dict['Samples Sequence Completed Pass'],
                               'formula': '=COUNT(DISTINCT(COLLECT(CHILDREN([Sample Name]@row), '
                                          'CHILDREN([Sequence Completed]@row), 1)))'})

    new_woid_row.cells.append({'column_id': column_ids_dict['Samples Sequence Completed Fail'],
                               'formula': '=COUNT(DISTINCT(COLLECT(CHILDREN([Sample Name]@row), '
                                          'CHILDREN([TKB Fail]@row), 1)))'})

    new_woid_row.to_top = True
    new_woid_row.parent_id = pr

    woid_row_response = ss_connector.Sheets.add_rows(sequence_review_sheet, [new_woid_row])

    for r in woid_row_response.data:
        return r.id


def create_sibling_row(parent_row_id, sample_dict, woid_billing_dict, sc, column_ids_dict):

    facilitator = update_admin_email(woid_billing_dict['Facilitator'], sc)
    f_values = [{'email': facilitator, 'name': facilitator}]

    new_row = sc.models.Row()

    new_row.cells.append({'column_id': column_ids_dict['Work Order ID'], 'value': sample_dict['WorkOrder']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Admin Project'], 'value': woid_billing_dict['Administration Project']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Sequence Completed'], 'value': sample_dict['Sequence Completed']})

    new_row.cells.append({'column_id': column_ids_dict['TKB Fail'], 'value': sample_dict['Seq Complete Status']})

    new_row.cells.append({'column_id': column_ids_dict['Library'], 'value': sample_dict['Library']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Sample Name'], 'value': sample_dict['Library'].split('-lib')[0]})

    new_row.cells.append({'column_id': column_ids_dict['Total Bases Kb (PF)'],
                          'value': float(sample_dict['Total Bases Kb (PF)'].replace(',', ''))})

    new_row.cells.append({'column_id': column_ids_dict['Index Sequence'], 'value': sample_dict['Index Sequence']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Library Input Barcode'], 'value': sample_dict['Library Input Barcode']})

    new_row.cells.append({'column_id': column_ids_dict['Index Summary Work Order'], 'value': sample_dict['WorkOrder']})

    new_row.cells.append({'column_id': column_ids_dict['Flow Cell'], 'value': sample_dict['FlowCell']})

    new_row.cells.append({'column_id': column_ids_dict['Lane'], 'value': sample_dict['Lane']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Data Count'], 'value': sample_dict['IndexIllumina Count']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Date Completed'], 'value': sample_dict['Date Completed']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Event Date'], 'objectValue': datetime.datetime.now().isoformat()})

    new_row.cells.append(
        {'column_id': column_ids_dict['Avg QScore (R1)'], 'value': float(sample_dict['Avg QScore (R1)'])})

    new_row.cells.append(
        {'column_id': column_ids_dict['Avg QScore (R2)'], 'value': float(sample_dict['Avg QScore (R2)'])})

    new_row.cells.append({'column_id': column_ids_dict['% >Q30 (R1)'], 'value': sample_dict['% >Q30 (R1)']})

    new_row.cells.append({'column_id': column_ids_dict['% >Q30 (R2)'], 'value': sample_dict['% >Q30 (R2)']})

    new_row.cells.append({'column_id': column_ids_dict['Facilitator'],
                          'objectValue': {'objectType': 'MULTI_CONTACT', 'values': f_values}})

    new_row.cells.append(
        {'column_id': column_ids_dict['Work Order Description'], 'value': woid_billing_dict['Description']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Pipeline'], 'value': woid_billing_dict['Pipeline']})

    new_row.to_bottom = True
    new_row.parent_id = parent_row_id

    return new_row


def update_sibling_row(sib_row_id, sample_dict, sc, column_ids_dict, status):

    s_complete = sample_dict['Sequence Completed']
    tkb_status = sample_dict['Seq Complete Status']
    if status:
        s_complete = status
        tkb_status = False

    new_row = sc.models.Row()

    new_row.cells.append(
        {'column_id': column_ids_dict['Sequence Completed'], 'value': s_complete})

    new_row.cells.append({'column_id': column_ids_dict['TKB Fail'], 'value': tkb_status})

    new_row.cells.append({'column_id': column_ids_dict['Flow Cell'], 'value': sample_dict['FlowCell']})

    new_row.cells.append({'column_id': column_ids_dict['Lane'], 'value': sample_dict['Lane']})

    new_row.cells.append({'column_id': column_ids_dict['Total Bases Kb (PF)'],
                          'value': float(sample_dict['Total Bases Kb (PF)'].replace(',', ''))})

    new_row.cells.append(
        {'column_id': column_ids_dict['Data Count'], 'value': sample_dict['IndexIllumina Count']})

    new_row.cells.append({'column_id': column_ids_dict['Date Completed'], 'value': sample_dict['Date Completed']})

    new_row.cells.append(
        {'column_id': column_ids_dict['Event Date'], 'objectValue': datetime.datetime.now().isoformat()})

    new_row.cells.append(
        {'column_id': column_ids_dict['Avg QScore (R1)'], 'value': float(sample_dict['Avg QScore (R1)'])})

    new_row.cells.append(
        {'column_id': column_ids_dict['Avg QScore (R2)'], 'value': float(sample_dict['Avg QScore (R2)'])})

    new_row.cells.append({'column_id': column_ids_dict['% >Q30 (R1)'], 'value': sample_dict['% >Q30 (R1)']})

    new_row.cells.append({'column_id': column_ids_dict['% >Q30 (R2)'], 'value': sample_dict['% >Q30 (R2)']})

    new_row.id = sib_row_id

    return new_row


def get_admin_mss_sheets(admin_project, ss_c):

    # get workspace id
    for space in ss_c.Workspaces.list_workspaces(include_all=True).data:

        if 'Smartflow Production Workspace' in space.name:
            workspace_id = space.id

    # get admin folder id
    for folders in ss_c.Workspaces.get_workspace(workspace_id).folders:
        if 'Admin Projects' in folders.name:
            admin_folder_id = folders.id

    # get active projects folder id
    for folders in ss_c.Folders.get_folder(admin_folder_id).folders:
        if 'Active Projects' in folders.name:
            active_projects_folder_id = folders.id

    # for all the folders in Active projects get all the sheet ids, add to a list
    population_admin_projects_folder_id_name = {}
    for folders in ss_c.Folders.get_folder(active_projects_folder_id).folders:
        if admin_project in folders.name:
            population_admin_projects_folder_id_name[folders.id] = folders.name

    # get mss sheet ids
    admin_project_sheet_ids = {}

    for folder_id in population_admin_projects_folder_id_name:
        admin_project_sheet_ids[population_admin_projects_folder_id_name[folder_id]] = []

        for sheets in ss_c.Folders.get_folder(folder_id).sheets:
            admin_project_sheet_ids[population_admin_projects_folder_id_name[folder_id]].append(sheets.id)

    return admin_project_sheet_ids


def update_mss_sheets_woid(i_data, woid_billing_dict, woid):

    print('Updating {} MSS sheets.'.format(woid_billing_dict['Administration Project']))
    ss_connector = ss_connect(conn='c')

    woid_mss_sheets = get_admin_mss_sheets(woid_billing_dict['Administration Project'], ss_connector)

    mss_required_columns = ['Work Order ID',
                            'Pipeline',
                            'Current Production Status',
                            'WOI Status',
                            'Sample Full Name',
                            'Sequencing Completed Date',
                            'Fail',
                            'Re-attempt',
                            'Aliquot Requested']

    woid_samples = [x.split('-lib')[0] for x in i_data.keys() if i_data[x]['Sequence Completed']]

    mss_data = {}
    total_samples = 0
    new_library = {}
    launch_samples = []

    for sheet_id in woid_mss_sheets[woid_billing_dict['Administration Project']]:

        updated_rows = []
        sheet_col_ids = {}

        # get required column id's to pull from sheet
        for col in ss_connector.Sheets.get_columns(sheet_id).data:
            if col.title in mss_required_columns:
                sheet_col_ids[col.title] = col.id

        mss_sheet = ss_connector.Sheets.get_sheet(sheet_id=sheet_id, column_ids=list(sheet_col_ids.values()))
        print(mss_sheet.name)

        for row in mss_sheet.rows:

            sample_found = False
            swoid_found = False
            status_found = False

            for cell in row.cells:

                if cell.column_id == sheet_col_ids['Work Order ID']:
                    swoid = cell.value
                    if swoid == woid:
                        swoid_found = True

                if cell.column_id == sheet_col_ids['Sample Full Name']:
                    sample_name = cell.value
                    if sample_name in woid_samples:
                        sample_found = True

                if cell.column_id == sheet_col_ids['Current Production Status']:
                    sample_production_status = cell.value
                    status_found = True

                if cell.column_id == sheet_col_ids['Fail']:
                    fail = cell.value

                if cell.column_id == sheet_col_ids['Re-attempt']:
                    attempt = cell.value

                if cell.column_id == sheet_col_ids['Aliquot Requested']:
                    aliquot = cell.value

            # if swoid_found and status_found and not sample_found:
            if swoid_found and status_found:

                total_samples += 1
                new_library[sample_name] = {'Fail': fail, 'Re-attempt': attempt, 'Aliquot Requested': aliquot}

                if sample_production_status not in mss_data:
                    mss_data[sample_production_status] = 1
                else:
                    mss_data[sample_production_status] += 1

            if sample_found and swoid_found:

                if sample_production_status == 'Sequencing Completed' or 'QC' in sample_production_status:
                    continue

                if sample_production_status != 'Sequencing Completed' or 'QC' not in sample_production_status:

                    mss_data[sample_production_status] -= 1

                    if 'Sequencing Completed' not in mss_data:
                        mss_data['Sequencing Completed'] = 1
                    else:
                        mss_data['Sequencing Completed'] += 1

                    new_row = ss_connector.models.Row()
                    new_row.id = row.id

                    new_cell = ss_connector.models.Cell()
                    new_cell.column_id = sheet_col_ids['Current Production Status']
                    new_cell.value = sequence_complete_status
                    new_row.cells.append(new_cell)

                    new_cell = ss_connector.models.Cell()
                    new_cell.column_id = sheet_col_ids['Sequencing Completed Date']
                    new_cell.value = datetime.datetime.now().isoformat()
                    new_row.cells.append(new_cell)

                    new_cell = ss_connector.models.Cell()
                    new_cell.column_id = sheet_col_ids['Pipeline']
                    new_cell.value = woid_billing_dict['Pipeline']
                    new_row.cells.append(new_cell)

                    new_cell = ss_connector.models.Cell()
                    new_cell.column_id = sheet_col_ids['WOI Status']
                    new_cell.value = woid_billing_dict['Status']
                    new_row.cells.append(new_cell)

                    updated_rows.append(new_row)
                    launch_samples.append(sample_name)

                    continue

        update = ss_connector.Sheets.update_rows(mss_sheet.id, updated_rows)
        print('{} samples updated\n'.format(len(updated_rows)))

    return mss_data, new_library, total_samples


def update_mss_sheets_sample(i_data, billing_wo_dict):

    admin_woid_sample_dict = {}
    sample_status = {}

    for woid, value in billing_wo_dict.items():

        if value['Administration Project'] not in admin_woid_sample_dict:
            admin_woid_sample_dict[value['Administration Project']] = {}

        if woid not in admin_woid_sample_dict[value['Administration Project']]:
            admin_woid_sample_dict[value['Administration Project']][woid] = []

        for data in i_data.values():
            sample_status[data['Sample Full Name']] = data['Sequence Completed']
            if data['WorkOrder'] in admin_woid_sample_dict[value['Administration Project']]:
                if data['Sample Full Name'] not in admin_woid_sample_dict[value['Administration Project']][
                   data['WorkOrder']]:
                    admin_woid_sample_dict[value['Administration Project']][data['WorkOrder']]\
                        .append(data['Sample Full Name'])

    for admin in admin_woid_sample_dict:

        print('\nUpdating {} MSS sheets:'.format(admin))

        ss_connector = ss_connect(conn='c')

        woid_mss_sheets = get_admin_mss_sheets(admin, ss_connector)

        mss_required_columns = ['Work Order ID',
                                'Pipeline',
                                'Current Production Status',
                                'WOI Status',
                                'Sample Full Name',
                                'Sequencing Completed Date']

        for sheet_id in woid_mss_sheets[admin]:

            updated_rows = []

            sheet_col_ids = {}

            # get required column id's to pull from sheet
            for col in ss_connector.Sheets.get_columns(sheet_id).data:
                if col.title in mss_required_columns:
                    sheet_col_ids[col.title] = col.id

            mss_sheet = ss_connector.Sheets.get_sheet(sheet_id=sheet_id, column_ids=list(sheet_col_ids.values()))
            print(mss_sheet.name)
            for row in mss_sheet.rows:

                sample_found = False
                swoid_found = False

                for cell in row.cells:

                    if cell.column_id == sheet_col_ids['Work Order ID']:
                        swoid = cell.value
                        swoid_found = True

                    if cell.column_id == sheet_col_ids['Sample Full Name']:
                        sample_name = cell.value
                        sample_found = True

                    if cell.column_id == sheet_col_ids['Current Production Status']:
                        sample_production_status = cell.value

                if sample_found and swoid_found:

                    if swoid in admin_woid_sample_dict[admin]:

                        if sample_name in admin_woid_sample_dict[admin][swoid]:

                            if sample_status[sample_name]:

                                if sample_production_status == 'Sequencing Completed' or 'QC' in \
                                        sample_production_status:
                                    continue

                                if sample_production_status != 'Sequencing Completed' or 'QC' not in \
                                        sample_production_status:

                                    new_row = ss_connector.models.Row()
                                    new_row.id = row.id

                                    new_cell = ss_connector.models.Cell()
                                    new_cell.column_id = sheet_col_ids['Current Production Status']
                                    new_cell.value = sequence_complete_status
                                    new_row.cells.append(new_cell)

                                    new_cell = ss_connector.models.Cell()
                                    new_cell.column_id = sheet_col_ids['Sequencing Completed Date']
                                    new_cell.value = datetime.datetime.now().isoformat()
                                    new_row.cells.append(new_cell)

                                    new_cell = ss_connector.models.Cell()
                                    new_cell.column_id = sheet_col_ids['Pipeline']
                                    new_cell.value = billing_wo_dict[swoid]['Pipeline']
                                    new_row.cells.append(new_cell)

                                    new_cell = ss_connector.models.Cell()
                                    new_cell.column_id = sheet_col_ids['WOI Status']
                                    new_cell.value = billing_wo_dict[swoid]['Status']
                                    new_row.cells.append(new_cell)

                                    updated_rows.append(new_row)

                                    continue

            update = ss_connector.Sheets.update_rows(mss_sheet.id, updated_rows)
            print('{} samples updated\n'.format(len(updated_rows)))


def pcc_update(data, admin_info, mss_data):

    woid = admin_info['Work Order']
    total_seq_samples = 0

    for s in mss_data:
        if s in ['QC Pass', 'QC Fail', 'Sequencing Completed']:
            total_seq_samples += int(mss_data[s])

    ssclient = ss_connect(conn='c')

    facilitator_email = update_admin_email(admin_info['Facilitator'], ssclient)

    s_pass = 0
    s_fail = 0
    for s in data:
        if data[s]['Sequence Completed']:
            s_pass += 1
        else:
            s_fail += 1

    col_dict = {}
    for col in ssclient.Sheets.get_columns(7495404104247172).data:
        col_dict[col.title] = col.id

    # production workspace
    pcc_sheet = ssclient.Sheets.get_sheet(7495404104247172)
    print('Updating {}:'.format(pcc_sheet.name))
    header_row_found = False
    woid_found = False

    for row in pcc_sheet.rows:
        for cell in row.cells:
            if cell.column_id == col_dict['Reporting Instance'] and cell.value == 'Sequence Completed Status':
                sequencing_parent_id = row.id
                header_row_found = True
                continue

            if header_row_found and cell.column_id == col_dict['Reporting Instance'] and cell.value == woid:
                woid_row_id = row.id
                woid_found = True

    if not woid_found:
        print('{} row not found, adding new row.'.format(woid))
        new_swo_row = ssclient.models.Row({"format": ",,,,,,,,,18,,,,,,"})
        new_swo_row.parent_id = sequencing_parent_id
        new_swo_row.to_top = True

        new_swo_row.cells.append({'column_id': col_dict['Reporting Instance'], 'value': woid, 'hyperlink': {
            'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(woid)}})
        new_swo_row.cells.append({'column_id': col_dict['Sequencing Work Order'], 'value': woid})
        new_swo_row.cells.append({'column_id': col_dict['Items'], 'value': total_seq_samples})
        new_swo_row.cells.append({'column_id': col_dict['Failure'], 'value': s_fail})
        new_swo_row.cells.append(
            {'column_id': col_dict['Admin Project'], 'value': admin_info['Administration Project'], 'hyperlink': {
                'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'.format(
                    admin_info['Administration Project'].replace(' ', '+'))}})
        # new_swo_row.cells.append({'column_id': col_dict['Facilitator'], 'object_value': admin_info['user email']})
        new_swo_row.cells.append({'column_id': col_dict['Facilitator'], 'object_value': {
            "objectType": 'MULTI_CONTACT', 'values': [{'email': facilitator_email,
                                                       'name': facilitator_email}]}})
        new_swo_row.cells.append({'column_id': col_dict['Event Date'], 'value': datetime.datetime.now().isoformat()})
        new_swo_row.cells.append({'column_id': col_dict['Production Notes'], 'value': sequence_complete_status})

        woid_row_response = ssclient.Sheets.add_rows(7495404104247172, [new_swo_row])

    else:
        print('{} row exists, updating row.'.format(woid))
        new_swo_row = ssclient.models.Row({"format": ",,,,,,,,,18,,,,,,"})
        new_swo_row.id = woid_row_id
        new_swo_row.cells.append({'column_id': col_dict['Items'], 'value': total_seq_samples})
        new_swo_row.cells.append({'column_id': col_dict['Failure'], 'value': s_fail})
        new_swo_row.cells.append({'column_id': col_dict['Production Notes'], 'value': sequence_complete_status})

        woid_row_response = ssclient.Sheets.update_rows(7495404104247172, [new_swo_row])

    return


def new_lib_status(parent_row, lib_data, woid):

    ss_con, scr_sheet, col_dict = ss_connect()

    woid_header_row_found = False
    sample_found = False
    work_order_found = False
    lib_update_rows = []

    for row in scr_sheet.rows:

        if row.id == parent_row:
            woid_header_row_found = True
            continue

        if woid_header_row_found:

            for cell in row.cells:

                if cell.column_id == col_dict['Work Order ID']:
                    if cell.value == woid:
                        work_order_found = True

                if cell.column_id == col_dict['Sample Name']:
                    if cell.value in lib_data:
                        sample = cell.value
                        sample_found = True

        if sample_found and work_order_found:

            new_lib_row = ss_con.models.Row()
            new_lib_row.id = row.id

            for item_, value in lib_data[sample].items():
                if value is not None:
                    new_lib_row.cells.append({'column_id': col_dict[item_], 'value': value})

            lib_update_rows.append(new_lib_row)

    ss_con.Sheets.update_rows(sequence_review_sheet, lib_update_rows)


sequence_review_sheet = 4880402662877060
if len(sys.argv) == 2 and sys.argv[1] == 'ls':
    print('\nUpdating Large Scale Projects Tracking Sheet\n')
    sequence_review_sheet = 8140804578404228

if len(sys.argv) == 2 and sys.argv[1] == 'of':
    print('\nUpdating Large Scale Projects Tracking Sheet: Overflow\n')
    sequence_review_sheet = 2023194224813956

print('Starting Sequencing Complete:\n')
total_kb_cutoff = input('Enter tkb (return for 63000000 default):\n')
if not total_kb_cutoff:
    total_kb_cutoff = 63000000
total_kb_cutoff = int(total_kb_cutoff)

print('\nTotal kb cutoff: {}\n'.format(total_kb_cutoff))

sequence_complete_status_dict = {'1': 'Sequencing Completed', '2': 'Initial Sequencing Completed'}
while True:
    for k, v in sorted(sequence_complete_status_dict.items()):
        print('{}: {}'.format(k, v))
    response = input('Enter sequence complete status (return to use 1):\n')
    if response in sequence_complete_status_dict:
        sequence_complete_status = sequence_complete_status_dict[response]
        break
    else:
        sequence_complete_status = 'Sequencing Completed'
        break

print('\nSequencing status: {}\n'.format(sequence_complete_status))

input_list = []

print('Enter woids or samples:')
while True:
    input_in = input()
    if input_in:
        input_list.append(input_in)
    else:
        break

input_list = [x.strip() for x in input_list]

for item in input_list:
    if '28' in item[:2] and len(item) == 7:
        print('\nStarting {} Sequencing Complete Update'.format(item))
        illumina_info_woid(item, total_kb_cutoff)
    else:
        print('\nStarting Sequencing Complete Samples Update')
        illumina_info_sample(','.join(input_list), total_kb_cutoff)
        break

print('\nSequencing Complete Update Finished.\n')
