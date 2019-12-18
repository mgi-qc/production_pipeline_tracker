__author__ = 'Thomas Antonacci'

"""
TODO: Catching bad input...
TODO: Input minimum; request input check box in smartsheet
"""

import smartsheet
import csv
import os
import sys
from datetime import datetime
import subprocess
import glob
import smrtqc


# Misc Functions
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
        print('Try Again: ')
        if try_count % 5 == 0 and try_count > 0:
            print('Please enter the Resource Bank work order id: ')
        try_count += 1


def get_total_dna_needed():
    """
    TODO: Get min dna req and standard req
    :return: total_in : total dna required for resource work order
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
        std_in = input()

        if is_num(std_in) and float(std_in) > 0:
            std_req = float(std_in)
        else:
            print('Please enter a positive number: ')


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


def get_project_name(inventory_file):
    """

    :param inventory_file: name of inventory file from LIMS
    :return: Admin Project name
    """
    with open(inventory_file, 'r') as in_file:
        i = 0
        while i < 6:
            next(in_file)
            i += 1

        admin_line = in_file.readline()
        if 'Administration Project' in admin_line:
            name = admin_line.split('\t')[1].strip().replace('"', '')
        else:
            exit('Unexpected Administration Project Location!')

        return name


def check_dna(total_dna, req_dna):
    """
    TODO: Check both std and min req dna

    :param total_dna: total dna assessed
    :param req_dna: dna required for pass
    :return: Pass or fail output
    """
    if is_num(total_dna):
        if float(total_dna) > float(req_dna):
            return 'Resource Assessment Pass'
        else:
            return 'Resource Assessment Fail'
    else:
        return 'Resource Assessment Complete'


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
        if cell.column_id == column_ids['Resource Work Order']:
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


def add_comment_to_PCC(prod_space, woid, ss_connector):
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
    if comment:
        ss_connector.smart_sheet_client.Discussions.create_discussion_on_row(prod_comm_sheet.id, wo_row.id,
                                                                             ss_connector.smart_sheet_client.models.
                                                                             Discussion({'comment': ss_connector.
                                                                                        smart_sheet_client.models.
                                                                                        Comment({'text': comment})}))


def main():
    """
    TODO: Make directory and workspace routing centralized to smartflow/smrtqc
    TODO: Build report from template provided
    TODO: Mark failed samples in smartsheet to kick off workflow
    """

    # Initialize smrtqc object
    ss_client = smrtqc.SmartQC(api_key=os.environ.get('SMRT_API'))

    # Change directories to smartflow
    os.chdir('/gscmnt/gc2746/production/smartflow/production_files/resource_bank')

    # get RB work order and Pass/Fail conditions from input
    woid = get_wo_id()
    total_dna_req = get_total_dna_needed()

    # get info from lims using query
    print('Getting Inventory Sheet from LIMS...')
    subprocess.run(['wo_info', '--woid', woid, '--report', 'inventory', '--format', 'tsv'])

    # Check for inventory file!
    files = glob.glob('inventory.tsv')
    if len(files) == 0:
        exit('Inventory file not found!')

    # convert tsv with ugly stuff at top to only header and rows
    inv_file = 'inventory.tsv'
    inv_std = make_std_file(inv_file)

    # get project name from inventory file
    proj_name = get_project_name(inv_file)

    # Use nice std tsv to read in data
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
        if folder.name == proj_name:
            samp_folder = ss_client.get_object(folder.id, 'f')

    sample_sheets = []

    print('Getting sample sheets...')

    # get sample sheets from project folder
    for sheet in samp_folder.sheets:
        if 'MSS' in sheet.name:
            sample_sheets.append(ss_client.get_object(sheet.id, 's'))

    # Search sheets for samples and update with assessed dna and status
    print('Updating Smartsheet')

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
            if row == 0 and column_ids == 0 and sheet == 0:

                print(line['Content_Desc'] + ' not found in sample sheets in Smartsheet.')

            else:

                assessment_cell = smartsheet.smartsheet.models.Cell()
                assessment_cell.column_id = column_ids['Resource Assessment Completed Date']
                assessment_cell.value = date

                pass_fail_cell = smartsheet.smartsheet.models.Cell()
                pass_fail_cell.column_id = column_ids['Current Production Status']

                pass_fail_cell.value = check_dna(total_dna=line['Total_DNA (ng)'], req_dna=total_dna_req)

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

    add_comment_to_PCC(op_space, woid, ss_client)

    # remove temps
    os.remove(inv_std)
    os.remove(inv_file)


if __name__ == '__main__':
    main()
