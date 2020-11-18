__author__ = 'Thomas Antonacci'

import smartsheet
import csv
import os
import sys
from datetime import datetime
import subprocess
import glob
import smrtqc
import argparse


def is_num(s):
    """
    Returns True if object can be cast as an int, False if not
    :param s: any object 's'
    :return: bool
    """

    try:
        float(s)
    except ValueError:
        return False
    return True


def update_admin_email(facilitator, s_con):
    """
    Author Lee Trani
    Get facilitator object from email sheet
    :param facilitator: user email
    :param s_con: smartsheet connection
    :return: user email object
    """

    user_sheet = s_con.get_object(252568824768388, 's')
    sheet_column_dict = get_column_ids(user_sheet)
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


def get_wo_id():
    """
    Input Resource Bank Work Order ID for assessment: has digit checks

    :return: wo_in : work order entered in by user
    """

    # Input prompt
    print('\nPlease enter the Resource Bank work order id: ')
    try_count = 0

    # Capture/recapture and check input
    while True:

        wo_in = input().strip()

        if len(wo_in) != 7:
            print('Work orders must be 7 digits long')
        elif not is_num(wo_in):
            print('Workorders must be made up of numbers only!')
        else:
            return wo_in

        if try_count % 5 == 0 and try_count > 0:
            print('Please enter the Resource Bank work order id: ')
        else:
            print('Try Again: ')
        try_count += 1


def get_total_dna_needed():
    """"
    Get and check minimum and standard amount of dna required for pass/borderline/fail from user input
    :return: min_req, std_req : tuple of minimum and standard amount of dna required for pass/borderline/fail
    """

    print('Please enter the minimum amount of dna needed for the sample to pass(in ng): ')

    while True:
        min_in = input()
        if is_num(min_in) and float(min_in) > 0:
            min_req = float(min_in)
            break
        else:
            print('Please enter a positive number: ')

    print('Please enter the standard amount of dna needed for the sample to pass(in ng): ')

    while True:
        std_req = input()
        if is_num(std_req):
            if float(std_req) > 0 and float(std_req) > min_req:
                std_req = float(std_req)
                break
            elif float(std_req) < float(min_req):
                print('Standard DNA amount required must be greater that the minimum')
            else:
                print('Please enter a positive number: ')
        else:
            print('Please enter a positive number: ')

    return min_req, std_req


def check_dna(total_dna, min_dna_req, mm):
    """
    Assess samples as pass/fail
    :param total_dna: total dna assessed
    :param min_dna_req: dna required for pass
    :param mm: list to keep track of pass values
    :return: Pass or fail output
    """

    if is_num(total_dna):
        mm.append(float(total_dna))
        if float(total_dna) > float(min_dna_req):
            return 'Resource Assessment Pass'
        else:
            return 'Resource Assessment Fail'
    else:
        return 'Resource Assessment Complete'


def update_counts(total_dna, counts, std_dna_req, min_dna_req):
    """
    Update pass fail counts
    :param total_dna: dna value from inventory query
    :param counts: count dict for result tracking
    :param std_dna_req: user input
    :param min_dna_req: user input
    :return:
    """
    result = False
    if is_num(total_dna):
        total_dna = float(total_dna)
        result = True
        if total_dna > std_dna_req:
            counts['std_pass'] += 1
        else:
            if total_dna > min_dna_req:
                counts['min_pass'] += 1
            else:
                if total_dna > 50:
                    counts['fail_min'] += 1
                else:
                    counts['fails'] += 1
    return result


def build_assessment_report(counts, std_req, min_req, total, proj_info, min_max):
    """
    Function edited/revised by Lee Trani, changes tracked in github.
    Make assessment report to be attached to smartsheet
    :param total: total number of samples
    :param counts: counts for samples passing min/std req dna
    :param std_req: std req dna amount for sample to pass
    :param min_req: min req dna amount for sample to pass
    :param proj_info: dictionary of project information
    :param min_max: list of total_dna values that pass for max/min comment
    :return: assessment file name
    """

    report = 'Hello,\n' \
             '\n' \
             'The assessment is complete and the new work order has been updated.\n' \
             '\n' \
             'DNA available for Sequencing Plan/Additional DNA request: \n' \
             '\t- Total # of Samples: {total}\n' \
             '\t- Suggested Sequencing Plan: {pipeline}\n' \
             '\t- # Samples Passed Based on Seq Plan Standard Amts ({std_req} ng): {std_pass}\n' \
             '\t- # Samples Passed Based on Seq Plan Min. Amts ({min_req} ng): {min_pass}\n' \
             '\t- # Samples Failed, Not Meeting Seq Plan Min. Amts(50-{olmin} ng): {min_fail}\n' \
             '\t- # Samples Failed (less than 50ng): {fails}\n' \
             '\n' \
             '\t- Comments:\n' \
             '\t  Total DNA ranges from ~{min}ng to ~{max}ng\n'.format(
                                    total=total, pipeline=proj_info['pipeline'],
                                    std_req=std_req, std_pass=counts['std_pass'],
                                    min_req=min_req, min_pass=counts['min_pass'],
                                    olmin=min_req - 1, min_fail=counts['fail_min'],
                                    fails=counts['fails'], min=min(min_max), max=max(min_max))

    out_file = '{desc}.txt'.format(desc=proj_info['description'].replace(' ', '_').replace('/', '-')).replace('(', '') \
        .replace(')', '')

    print('Resource Assessment Report:\n{}'.format(report))
    comment = input('\nAdd comment to report/resource bank work order in PCC (Enter to continue without comment)?:\n')

    with open(out_file, 'w') as fout:
        fout.write('{}\t  {}\n'.format(report, comment))

    comment = 'Total DNA ranges from ~{min}ng to ~{max}ng\n{c}'.format(min=min(min_max), max=max(min_max), c=comment)

    return [out_file, comment]


def lims_data(woid, inventory_file, min_dna_req, std_dna_req, ss_con):
    """
    Function edited/revised by Lee Trani, changes tracked in github.
    :param woid: work order
    :param inventory_file: name of inventory file from LIMS
    :param min_dna_req: minimum DNA required
    :param std_dna_req: standard DNA required
    :param ss_con: smartsheet connection object
    :return: project information, sample data, report
    """

    with open(inventory_file, 'r') as in_file, open('{}.inventory.csv'.format(woid), 'w') as outfile:

        infile_reader = csv.reader(in_file, delimiter='\t')

        counts = {'std_pass': 0, 'min_pass': 0, 'fail_min': 0, 'fails': 0}
        project_dictionary = {'Admin Project': 'NA', 'description': 'NA', 'pipeline': 'NA',
                              'Event Date': datetime.now().isoformat(),
                              'Production Notes': 'Resource Assessment Completed', 'Reporting Instance': woid}
        data = {}

        header_found = False
        inventory_history = False
        min_max_list = []

        for line in infile_reader:

            if len(line) == 0 or '-' in line[0]:
                continue

            line = [x.replace('"', '') for x in line]

            if 'Administration Project' in line[0]:
                project_dictionary['Admin Project'] = line[1]

            if 'Description:' in line[0]:
                project_dictionary['description'] = line[1]

            if 'WO Facilitator' in line[0]:
                facilitator = update_admin_email(line[1], ss_con)
                project_dictionary['Facilitator'] = [{'email': facilitator, 'name': facilitator}]

            if 'Pipeline' in line[0]:
                project_dictionary['pipeline'] = line[1]

            if '#' in line[0] and not header_found:
                header = line
                if 'Content_Desc' not in header or 'Total_DNA (ng)' not in header:
                    sys.exit('Inventory file header not correct, missing Content_Desc or Total_DNA (ng).')
                outfile_writer = csv.DictWriter(outfile, fieldnames=header + ['resource_assessment'])
                outfile_writer.writeheader()
                header_found = True
                continue

            if 'Inventory History' in line:
                inventory_history = True

            if header_found and not inventory_history:
                line_dict = dict(zip(header, line))
                line_dict['resource_assessment'] = check_dna(total_dna=line_dict['Total_DNA (ng)'],
                                                             min_dna_req=min_dna_req, mm=min_max_list)
                data[line_dict['Content_Desc']] = line_dict
                result = update_counts(total_dna=line_dict['Total_DNA (ng)'], counts=counts, std_dna_req=std_dna_req,
                                       min_dna_req=min_dna_req)

                if not result:
                    print('Sample {} has no Total_DNA (ng)'.format(line_dict['Content_Desc']))

                outfile_writer.writerow(line_dict)

    project_dictionary['Items'] = len(data)
    project_dictionary['Failure'] = counts['fail_min'] + counts['fails']

    report_comment_list = build_assessment_report(counts, std_dna_req, min_dna_req, len(data), project_dictionary,
                                                  min_max_list)
    report_comment_list.append('{}.inventory.csv'.format(woid))

    return project_dictionary, data, report_comment_list


def get_mss_sheets(admin, ss_client):
    """
    Get MSS sheets and PCC sheet for updates
    :param admin: Admin project to get MSS sheets from
    :param ss_client: smartsheet object
    :return: list of MSS sheets
    """

    # get sample sheets using project name
    for space in ss_client.get_workspace_list():
        if space.name == 'Smartflow Production Workspace':
            op_space = ss_client.get_object(space.id, 'w')
            for sheet in op_space.sheets:
                if sheet.name == 'Production Communications Center':
                    pcc_sheet = ss_client.get_object(sheet.id, 's')

    # get projects folder
    for folder in op_space.folders:
        if folder.name == 'Admin Projects':
            projs_fold = ss_client.get_object(folder.id, 'f')

    # get active folder
    for folder in projs_fold.folders:
        if folder.name == 'Active Projects':
            active_folder = ss_client.get_object(folder.id, 'f')

    # get project samples folder
    for folder in active_folder.folders:
        if folder.name == admin[:50]:
            samp_folder = ss_client.get_object(folder.id, 'f')

    sheets = []

    # get sample sheets from project folder
    for sheet in samp_folder.sheets:
        if 'MSS' in sheet.name:
            sheets.append(ss_client.get_object(sheet.id, 's'))

    return sheets, pcc_sheet


def get_column_ids(sheet):
    """
    Get column ids for sheets
    :param sheet: smartsheet sheet object
    :return: dict of column ids
    """

    column_ids = {}
    for col in sheet.columns:
        column_ids[col.title] = col.id

    return column_ids


def update_samples(sample_data, sample_sheets, woid, ss_client):
    """
    Function edited/revised by Lee Trani, changes tracked in github.
    :param sample_data: lims query sample dict
    :param sample_sheets: MSS sheets
    :param woid: woid query
    :param ss_client: smartsheet object
    """

    for sheet in sample_sheets:

        print('{}'.format(sheet.name))

        column_ids = get_column_ids(sheet)

        rows_to_update = []

        for row in sheet.rows:

            wo_found = False
            sample_found = False

            for cell in row.cells:

                if cell.column_id == column_ids['Resource Storage']:
                    if cell.value == woid:
                        wo_found = True

                if cell.column_id == column_ids['Sample Full Name']:
                    if cell.value in sample_data:
                        sample = cell.value
                        sample_found = True

            if sample_found and wo_found:
                new_row = smartsheet.smartsheet.models.Row()
                new_row.id = row.id

                assessment_cell = smartsheet.smartsheet.models.Cell()
                assessment_cell.column_id = column_ids['Resource Assessment Completed Date']
                assessment_cell.value = datetime.now().isoformat()
                new_row.cells.append(assessment_cell)

                pass_fail_cell = smartsheet.smartsheet.models.Cell()
                pass_fail_cell.column_id = column_ids['Current Production Status']
                pass_fail_cell.value = sample_data[sample]['resource_assessment']
                new_row.cells.append(pass_fail_cell)

                rows_to_update.append(new_row)

        print('Samples Updated: {}'.format(len(rows_to_update)))
        if len(rows_to_update) > 0:
            ss_client.smart_sheet_client.Sheets.update_rows(sheet.id, rows_to_update)


def add_report_to_pcc(pcc_sheet, proj_info, ss_connector, rc):
    """
    Function edited/revised by Lee Trani, changes tracked in github.
    Add resource results to Resource Assessment Reports row in Production Communications Center Sheet.
    :param pcc_sheet: Production Workspace Sheet Object
    :param proj_info: associated project info
    :param ss_connector: smartsheet client object
    :param rc: resource report, comment, inventory.csv
    """

    col_ids = get_column_ids(pcc_sheet)

    parent_row_found = False
    for row in pcc_sheet.rows:

        if row.id == 5660392586798980:
            parent_row_found = True
            continue

        if parent_row_found:
            for cell in row.cells:
                if col_ids['Reporting Instance'] == cell.column_id:
                    if cell.value == proj_info['Reporting Instance']:
                        ss_connector.smart_sheet_client.Sheets.delete_rows(pcc_sheet.id, row.id)
                    if cell.value == 'LC dilution drop-off':
                        parent_row_found = False
                        break

    new_row = smartsheet.smartsheet.models.Row({"format": ",,,,,,,,,18,,,,,,"})
    new_row.parent_id = 5660392586798980
    new_row.to_top = True

    for header, value in proj_info.items():

        if header in ['description', 'pipeline']:
            continue

        if header == 'Reporting Instance':
            new_row.cells.append({'column_id': col_ids[header], 'value': value, 'hyperlink': {
                'url': 'https://imp-lims.ris.wustl.edu/entity/setup-work-order/{}'.format(value)}})
            continue

        if header == 'Admin Project':
            new_row.cells.append({'column_id': col_ids[header], 'value': value, 'hyperlink': {
                'url': 'https://imp-lims.ris.wustl.edu/entity/administration-project/?project_name={}'.format(
                    value.replace(' ', '+'))}})
            continue

        if header == 'Facilitator':
            new_row.cells.append({'column_id': col_ids[header], 'objectValue': {'objectType': 'MULTI_CONTACT',
                                                                                'values': value}})
            continue

        new_row.cells.append({'column_id': col_ids[header], 'value': value})

    result = ss_connector.smart_sheet_client.Sheets.add_rows(pcc_sheet.id, [new_row]).data

    for r in result:
        new_row_id = r.id

    if rc[1]:
        ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(pcc_sheet.id, new_row_id,
                                                                             ss_connector.smart_sheet_client.models.
                                                                             Discussion({'comment': ss_connector.
                                                                                        smart_sheet_client.models.
                                                                                        Comment({'text': rc[1]})}))
    # Attach reports to resource work order
    print('\nAttaching report file to Work Order Initiation row in PCC.')
    ss_connector.smart_sheet_client.Attachments.attach_file_to_row(pcc_sheet.id, new_row_id, (rc[0], open(rc[0]), 'rb'))
    ss_connector.smart_sheet_client.Attachments.attach_file_to_row(pcc_sheet.id, new_row_id, (rc[2], open(rc[2], 'rb'),
                                                                                              'application/EXCEL'))


def update_confluence():
    return


def main():
    """
    main functions edited/updated/revised by Lee Trani, changes tracked in github.
    """

    # Set dev option
    parser = argparse.ArgumentParser()
    parser.add_argument('-dev', help='Used for development and testing purposes', action='store_true')
    args = parser.parse_args()

    # Initialize smrtqc object
    ss_client = smrtqc.SmartQC(api_key=os.environ.get('SMRT_API'))

    # get RB work order, Pass/Fail conditions, user comment from input
    woid = get_wo_id()
    min_dna_req, std_dna_req = get_total_dna_needed()

    # get info from lims using query
    print('\nRunning wo_info inventory query.')
    subprocess.run(['wo_info', '--woid', woid, '--report', 'inventory', '--format', 'tsv'])

    # Check for inventory file!
    if not os.path.isfile('inventory.tsv'):
        sys.exit('Inventory file not found!')

    # get project billing info, sample data, generate report file
    proj_info, sample_data, report_comment = lims_data(woid, 'inventory.tsv', min_dna_req, std_dna_req, ss_client)

    # get MSS sheets, op space id
    sample_sheets, pcc_sheet = get_mss_sheets(proj_info['Admin Project'], ss_client)

    print('\nUpdating Smartsheet\nAdmin Project: {}'.format(proj_info['Admin Project']))

    # update samples in MSS sheets
    update_samples(sample_data, sample_sheets, woid, ss_client)

    # Add report to work order row in Production Communication Center
    add_report_to_pcc(pcc_sheet, proj_info, ss_client, report_comment)

    print('\nReport file:\n{}\n\nInventory file:\n{}\n\nRessource Assessment Complete.'.format(report_comment[0],
                                                                                               report_comment[2]))
    # remove files
    os.remove('inventory.tsv')
    os.remove(report_comment[0])
    os.remove(report_comment[2])


if __name__ == '__main__':
    main()
