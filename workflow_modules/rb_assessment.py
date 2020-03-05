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


def get_wo_id():
    """
    Input Resource Bank Work Order ID for assessment: has digit checks

    :return: wo_in : work order entered in by user
    """

    # Input prompt
    print('Please enter the Resource Bank work order id: ')
    try_count = 0

    # Capture/recapture and check input
    while True:

        wo_in = input()

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

    req_list = [min_req, std_req]
    return req_list


def make_std_file(inventory_file):
    """

    :param inventory_file: name of inventory file from LIMS
    :return: inventory file that has been standardized
    """
    inventory_std = inventory_file.split('.')[0] + '_std.' + inventory_file.split('.')[1]

    with open(inventory_file, 'r') as in_file, open(inventory_std, 'w') as out_file:

        line_num = 1
        for line in in_file:
            if line_num >= 24 and line_num != 25:
                line = line.replace('"', '')
                out_file.write(line)
            line_num += 1
    return inventory_std


def get_project_info(inventory_file):
    """
    Make dictionary
    :param inventory_file:
    :return:
    """

    project_dictionary = {}

    with open(inventory_file, 'r') as in_file:

        for line in in_file:

            if 'Administration Project' in line:
                project_dictionary['name'] = line.split('\t')[1].strip().replace('"', '')
            elif 'Description:' in line:
                project_dictionary['description'] = line.split('\t')[1].replace('"', '')
            elif 'Pipeline' in line:
                project_dictionary['pipeline'] = line.split('\t')[1].replace('"', '')
        return project_dictionary




def check_dna(total_dna, min_dna_req):
    """

    :param total_dna: total dna assessed
    :param req_dna: dna required for pass
    :return: Pass or fail output
    """
    if is_num(total_dna):
        if float(total_dna) > float(min_dna_req):
            return 'Resource Assessment Pass'
        else:
            return 'Resource Assessment Fail'

    else:
        return 'Resource Assessment Complete'


def update_counts(total_dna, counts, std_dna_req, min_dna_req):

    if is_num(total_dna):
        total_dna = float(total_dna)
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


def get_sample_row_sheet(line_dict, sample_sheets, wo):
    """

    returns row, sheet and column ids if sample in line is found, else returns 0,0,0
    :param line_dict: line from dict reader
    :param sample_sheets: list of sample sheets to be searched
    :param wo: work order associated with sample
    :return: row, sheet, and column ids
    """
    for sheet in sample_sheets:

        column_ids = {}
        for col in sheet.columns:
            column_ids[col.title] = col.id

        for row in sheet.rows:
            if check_row_for_sample(line_dict['Content_Desc'], row=row, column_ids=column_ids, woid=wo):
                return row, sheet, column_ids
        return 0, 0, 0


def check_row_for_sample(sample, row, column_ids, woid):
    """
    Returns True if sample found, false if not

    :param sample: sample name
    :param woid: work order id
    :param row: row object
    :param column_ids: column ids from current sheet
    :return: Bool
    """
    wo_found = False
    sample_found = False

    for cell in row.cells:
        if cell.column_id == column_ids['Resource Storage']:
            if cell.value == woid:
                wo_found = True

    for cell in row.cells:
        if cell.column_id == column_ids['Sample Full Name']:
            if cell.value == sample:
                sample_found = True

    if wo_found and sample_found:
        return True
    else:
        return False


def check_header(given_header):
    """
    Checks header for inventory file

    :param given_header: Header for current inventory sheet
    :return: None
    """

    expected_header = ['#', 'Barcode', 'Content_Desc', 'Common_Name', 'DNA_Type', 'Volume (ul)',
                       'Concentration (ng/ul)', 'Molarity (nM)', 'Total_DNA (ng)', 'Freezer_Loc',
                       'Freezer Status', 'Freezer_Group', 'Tissue Name', 'Tissue Type']

    for head in expected_header:
        if head not in given_header:
            print('{} missing from inventory header!'.format(head))
            sys.exit('Unexpected header!')


def add_report_to_PCC(prod_space, woid, ss_connector, report_file):
    """
    Add comment to Production Communications Center Sheet in Production Workspace

    :param prod_space: Production Workspace Object
    :param woid: work order id of this assessment
    :param ss_connector: smartsheet client object
    :return: None
    """
    for sheet in prod_space.sheets:
        if sheet.name == 'Production Communications Center':
            prod_comm_sheet = sheet

    prod_comm_sheet = ss_connector.get_object(prod_comm_sheet.id, 's')

    for row in prod_comm_sheet.rows:
        for cell in row.cells:
            if woid == cell.value:
                wo_row = row

    # Graciously provided by @ltrani
    comment = input('\nAdd comment to Resource Bank Work Order in PCC (Enter to continue without comment):\n')
    # comment = 'Making some comments yo'
    # debug if comment:
    #     pass
    #     ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(prod_comm_sheet.id, wo_row.id,
    #                                                                          ss_connector.smart_sheet_client.models.
    #                                                                          Discussion({'comment': ss_connector.
    #                                                                                     smart_sheet_client.models.
    #                                                                                     Comment({'text': comment})}))
    # Attach report to resource work order
    attached_file = ss_connector.smart_sheet_client.Attachments.attach_file_to_row(prod_comm_sheet.id, wo_row.id, (report_file, open(report_file), 'rb'))


def update_samples(inv_std, sample_sheets, woid, std_dna_req, min_dna_req, op_space, ss_client, proj_info):

    counts = {'std_pass': 0,
              'min_pass': 0,
              'fail_min': 0,
              'fails': 0}

    with open(inv_std, 'r') as inventory_file:
        reader = csv.DictReader(inventory_file, delimiter='\t')
        header = reader.fieldnames

        check_header(header)

        # dict of lists of rows to update using sheet id as header
        rows_to_update = {}
        count = 0

        date = datetime.now().strftime('%Y-%m-%d')

        # iterate over samples, assess pass/fails, and load results into smartsheet
        for line in reader:

            row, sheet, column_ids = get_sample_row_sheet(line, sample_sheets, woid)
            update_counts(total_dna=line['Total_DNA (ng)'], counts=counts, std_dna_req=std_dna_req, min_dna_req=min_dna_req)
            if row == 0 and column_ids == 0 and sheet == 0:

                print(line['Content_Desc'] + ' not found in sample sheets in Smartsheet.')

            else:

                assessment_cell = smartsheet.smartsheet.models.Cell()
                assessment_cell.column_id = column_ids['Resource Assessment Completed Date']
                assessment_cell.value = date

                pass_fail_cell = smartsheet.smartsheet.models.Cell()
                pass_fail_cell.column_id = column_ids['Current Production Status']

                pass_fail_cell.value = check_dna(total_dna=line['Total_DNA (ng)'], min_dna_req=min_dna_req)

                new_row = smartsheet.smartsheet.models.Row()
                new_row.id = row.id
                new_row.cells.append(assessment_cell)
                new_row.cells.append(pass_fail_cell)
                if sheet.id in rows_to_update:
                    rows_to_update[sheet.id].append(new_row)
                else:
                    rows_to_update[sheet.id] = [new_row]
                count += 1

    for sid in rows_to_update:
        updated_rows = ss_client.smart_sheet_client.Sheets.update_rows(sid, rows_to_update[sid])

    report_file = build_assessment_report(counts, std_dna_req, min_dna_req, proj_info)

    return report_file


def build_assessment_report(counts, std_req, min_req, proj_info):
    """
    Make assessment report to be attached to smartsheet

    :param counts: counts for samples passing min/std req dna
    :param std_req: std req dna amount for sample to pass
    :param min_req: min req dna amount for sample to pass
    :param proj_info: dictionary of project information
    :return: assessment file name
    """
    comments = ''
    total = 0
    for item in counts:
        total += counts[item]

    report = 'Hello,\n' \
             '\n' \
             'The assessment is complete and the new work order has been updated.\n' \
             '\n' \
             'DNA available for Sequencing Plan/Additional DNA request: \n' \
             '\t- Total # of Samples: {total}' \
             '\t- Suggested Sequencing Plan: {pipeline}' \
             '\t- # Samples Passed Based on Seq Plan Standard Amts ({std_req} ng): {std_pass}\n' \
             '\t- # Samples Passed Based on Seq Plan Min. Amts ({min_req} ng): {min_pass}\n' \
             '\t- # Samples Failed, Not Meeting Seq Plan Min. Amts(50-{olmin} ng): {min_fail}\n' \
             '\t- # Samples Failed (less than 50ng): {fails}\n' \
             '\n' \
             '\t- Comments: {comment}'.format(
              total=total, pipeline=proj_info['pipeline'],
              std_req=std_req, std_pass=counts['std_pass'],
              min_req=min_req, min_pass=counts['min_pass'],
              olmin=min_req - 1, min_fail=counts['fail_min'],
              fails=counts['fails'],
              comment=comments)

    report_file = '{desc}.txt'.format(desc=proj_info['description'].replace(' ', '_'))

    with open(report_file.format(), 'w') as fout:
        fout.write(report)

    return report_file


def main():
    """
    TODO: Add report to smartsheet resource work order row in PCC
    TODO: Mark failed samples in smartsheet to kick off workflow
    """

    # Set dev option
    parser = argparse.ArgumentParser()
    parser.add_argument('-dev', help='Used for development and testing purposes', action='store_true')
    args = parser.parse_args()

    # Initialize smrtqc object
    ss_client = smrtqc.SmartQC(api_key=os.environ.get('SMRT_API'))

    # Change directories to smartflow
    os.chdir(ss_client.get_working_directory('rb'))

    # get RB work order and Pass/Fail conditions from input
    woid = get_wo_id()
    req_list = get_total_dna_needed()
    min_dna_req = req_list[0]
    std_dna_req = req_list[1]

    # get info from lims using query
    print('Getting Inventory Sheet from LIMS...')
    subprocess.run(['wo_info', '--woid', woid, '--report', 'inventory', '--format', 'tsv'])

    # Check for inventory file!
    files = glob.glob('inventory.tsv')
    if len(files) == 0:
        exit('Inventory file not found!')

    # convert tsv with ugly stuff at top to only header and rows
    # TODO This needs to be removed and the header should be handled wherever the sheet is read in
    inv_file = 'inventory.tsv'
    inv_std = make_std_file(inv_file)

    # get project name from inventory file
    proj_info = get_project_info(inv_file)

    # Use nice std tsv to read in data
    # TODO - use sheet produced by query and hanlde file header here
    inv_temp = inv_file.split('.')[0] + '_temp.' + inv_file.split('.')[1]

    # get sample sheets using project name
    for space in ss_client.get_workspace_list():
        if space.name == 'Smartflow Production Workspace':
            op_space = ss_client.get_object(space.id, 'w')

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
        if folder.name == proj_info['name']:
            samp_folder = ss_client.get_object(folder.id, 'f')

    sample_sheets = []

    print('Getting sample sheets...')

    # get sample sheets from project folder
    for sheet in samp_folder.sheets:
        if 'MSS' in sheet.name:
            sample_sheets.append(ss_client.get_object(sheet.id, 's'))

    # Search sheets for samples and update with assessed dna and status
    print('Updating Smartsheet')
    report_file = update_samples(inv_std, sample_sheets, woid, std_dna_req, min_dna_req, op_space, ss_client, proj_info)

    # Add report to work order row in Production Communication Center
    add_report_to_PCC(op_space, woid, ss_client, report_file)

    # remove temps
    os.remove(inv_std)
    os.remove(inv_file)


if __name__ == '__main__':
    main()
