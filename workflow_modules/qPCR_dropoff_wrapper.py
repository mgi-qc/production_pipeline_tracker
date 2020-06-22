#!/usr/bin/python3

"""
This wrapper script MUST be run on a docker image with access to lims commands
"""

import smartsheet
import sys
import os
import subprocess
import csv
import time
import smrtqc
import argparse
import datetime
import glob
import shutil


def get_bc_input():
    """

    :return: file name of barcode list
    """
    print('Please paste in barcodes from Dilution Drop Off (Enter "return q return" when finished): ')

    bcs = []
    while True:
        bc_line = input()
        if bc_line != 'q':
            bcs.append(bc_line.strip())
        else:
            break

    bc_file = 'barcodes.fof'
    with open(bc_file, 'w') as fout:
        for bc in bcs:
            fout.write(bc + '\n')

    return bc_file


def load_dropoff_into_smartsheet(dropoff_sheet, wo_sheet, info, smartsheet_obj, args, num_samples):
    """
    Load dropoff sheet generated by the LIMS query into Production Communication Center

    :param dropoff_sheet: name of the dropoff sheet file
    :param wo_sheet: name of the work order file generated by LIMS
    :param info: dictionary of dictionarys and lists loaded using the dropoff file from LIMS
    :param smartsheet_obj: SmrtQC object containing smartsheet functions and client
    :param args: args from ArgsParser
    :param num_samples: number of samples being updated
    :return: None
    """

    print('\nUpdating Production Communciations Sheet')
    print('PCC url:\n{}\n'.format('https://app.smartsheet.com/sheets/xHV2MrC8v32F28WH6Vv7RJ4CrjHrwxmpmR6VvJJ1'))

    for space in smartsheet_obj.get_workspace_list():
        if space.name == 'Smartflow Production Workspace':
            wrksp = smartsheet_obj.get_object(space.id, 'w')

    for sheet in wrksp.sheets:
        if sheet.name == 'Production Communications Center':
            prod_sheet = smartsheet_obj.get_object(sheet.id, 's')

    column_ids = smartsheet_obj.get_column_ids(prod_sheet.id)

    for row in prod_sheet.rows:
        for cell in row.cells:
            if cell.column_id == column_ids['Reporting Instance']:
                if cell.value == 'qPCR dilution drop-off' and args.q:
                    header_row = row
                    prod_note = 'qPCR dilution drop-off'
                elif cell.value == 'capture drop-off' and args.c:
                    header_row = row
                    prod_note = 'capture drop-off'


    # get list of facilitators involved in this dropoff
    facil_list = []
    facilitators = []
    for facil in info['Outgoing Queue Work Order Facilitator']:
        for person in facil.split(','):
            if person not in facilitators:
                facilitators.append(person)
                facil_list.append({"email": update_admin_email(smartsheet_obj, person), "name": person})

    # Build new cells for
    new_row = smartsheet.smartsheet.models.Row({"format": ",,,,,,,,,18,,,,,,"})
    new_row.parent_id = header_row.id
    new_row.to_top = True
    new_row.cells.append({'column_id': column_ids['Reporting Instance'], 'value': info['Title']})
    new_row.cells.append({'column_id': column_ids['Sequencing Work Order'], 'value': ', '.join(info['Outgoing Queue Work Order'])})
    new_row.cells.append({'column_id': column_ids['Admin Project'], 'value': ', '.join(info['Administration Project'])})
    new_row.cells.append({'column_id': column_ids['Items'], 'value': num_samples})
    new_row.cells.append({"columnId": column_ids['Facilitator'], "objectValue": {"objectType": "MULTI_CONTACT", "values": facil_list}})
    new_row.cells.append({'column_id': column_ids['Event Date'], 'value': info['Date']})
    new_row.cells.append({'column_id': column_ids['Production Notes'], 'value': prod_note})

    new_row_response = smartsheet_obj.smart_sheet_client.Sheets.add_rows(prod_sheet.id, [new_row]).data[0]

    attached_file = smartsheet_obj.smart_sheet_client.Attachments.attach_file_to_row(prod_sheet.id, new_row_response.id, (dropoff_sheet, open(dropoff_sheet), 'rb'))


def get_info(dropoff, wo_sheet):
    """
    Parse and retrive information needed from dropoff generated by LIMS

    :param dropoff: aforementioned dropoff sheet
    :param wo_sheet: work order sheet generated by LIMS
    :return: info_dict
    """

    temp_file = 'temp_inv_file'
    with open(temp_file, 'w') as fout1, open(dropoff, 'r') as fin1:

        stop = False

        for line in fin1:

            if line == '\n':
                stop = True
            elif not stop:
                fout1.write(line)

    with open(temp_file, 'r') as fin1, open(wo_sheet, 'r')as fin2:
        title = fin1.readline().strip().replace('"', '')
        next(fin2)
        reader1 = csv.DictReader(fin1, delimiter='\t')
        reader2 = csv.DictReader(fin2, delimiter='\t')

        next(reader2)
        next(reader1)

        # list of work orders
        info_dict = {'Outgoing Queue Work Order': [], 'Administration Project': [], 'Outgoing Queue Work Order Facilitator': [], 'Date': time.strftime('%Y-%m-%d'), 'Title': title, 'search_info': {}}
        dropoff_list_o_headers = ['Outgoing Queue Work Order Facilitator', 'Outgoing Queue Work Order']
        wo_list_o_headers = ['Administration Project']

        try:
            for line in reader1:
                for header in dropoff_list_o_headers:
                    if line[header] == '' or line[header] is None:
                        raise KeyError
                    if line[header] not in info_dict[header]:
                        info_dict[header].append(line[header])

            for line in reader2:
                if line['Work Order'] in info_dict['Outgoing Queue Work Order']:
                    if line['Administration Project'] not in info_dict['Administration Project']:
                        info_dict['Administration Project'].append(line['Administration Project'])

        except KeyError:
            sys.exit('Work Order not found; Check dilution_drop_off.tsv.')

    os.remove(temp_file)
    return info_dict


def send_get_sheets(file):
    """
    Generate work order and dilution drop offs

    :param file: barcodes file
    :return: None
    """
    print('Emailing Dropoff report:')
    # barcode_info -r dilution_drop_off -bc-file ~/barcode.fof -format email
    subprocess.run(['barcode_info', '-r', 'dilution_drop_off', '--bc-file', file, '-format', 'email'])

    print('Getting Dropoff report:')
    # barcode_info -r dilution_drop_off -bc-file ~/barcode.fof -format tsv
    subprocess.run(['barcode_info', '-r', 'dilution_drop_off', '--bc-file', file, '-format', 'tsv'])

    print('Getting Work Order report:')
    # barcode_info -r work_order -bc-file ~/barcode.fof -format tsv
    subprocess.run(['barcode_info', '-r', 'work_order', '--bc-file', file, '-format', 'tsv'])

    # Check for files before continuing
    if not os.path.exists('work_order.tsv'):
        sys.exit('Work Order file creation failed; please contact mgi-qc')
    if not os.path.exists('dilution_drop_off.tsv'):
        sys.exit('Drop-off file creation failed; please contact mgi-qc')


def get_sample_inventories(admin_dict):
    """
    Get sample inventories containing

    :param admin_dict: list of
    :return: sample_inventories - dictionary of dictionaies of lists of samples to work orders to admin projects (for ease of searching later)
    """

    sample_inventories = {}

    for admin in admin_dict:
        for wo in admin_dict[admin]:
            subprocess.run(['barcode_info', '--report', 'sample_inventory', '--bc', ','.join(admin_dict[admin][wo]), '--format', 'tsv'])

            with open('sample_inventory.tsv', 'r') as inv_file:
                next(inv_file)
                inv_reader = csv.DictReader(inv_file, delimiter='\t')

                next(inv_reader)
                for line in inv_reader:
                    if line['DNA_Type'] != 'Pooled Library' and line['DNA_Type'] != 'pooled library':
                        if admin not in sample_inventories:
                            sample_inventories[admin] = {}
                        if wo not in sample_inventories[admin]:
                            sample_inventories[admin][wo] = []
                        sample_inventories[admin][wo].append(line['DNA'])

    return sample_inventories


def update_sample_statuses(dil_drop, info, smartsheet_cl, sample_status):

    ss_headers = ['Work Order ID', 'Sample Full Name']

    # get dictionary of barcodes to workorders to admin projects
    admin_wo_bc_dict = read_in_work_order('work_order.tsv', info)
    # get dictionary of samples to work orders to admin projects
    samples_dict = get_sample_inventories(admin_wo_bc_dict)

    for wrksp in smartsheet_cl.get_workspace_list():
        if wrksp.name == 'Smartflow Production Workspace':
            ophub = wrksp

    for folder in smartsheet_cl.get_folder_list(ophub.id, 'w'):
        if folder.name == 'Admin Projects':
            admin_folder = folder

    for folder in smartsheet_cl.get_folder_list(admin_folder.id, 'f'):
        if folder.name == 'Active Projects':
            projects_folder = folder

    for folder in smartsheet_cl.get_folder_list(projects_folder.id, 'f'):
        if folder.name in samples_dict:
            for sheet in smartsheet_cl.get_object(folder.id, 'f').sheets:
                rows_to_update = []

                if 'MSS' in sheet.name:
                    col_ids = smartsheet_cl.get_column_ids(sheet.id)
                    ids = []
                    for header in col_ids:
                        if header in ss_headers:
                            ids.append(col_ids[header])

                    mss_sheet = smartsheet_cl.get_sheet_with_columns(sheet.id, ids)
                    for row in mss_sheet.rows:
                        sample_found = False
                        wo_found = False

                        for cell in row.cells:
                            if cell.column_id == col_ids['Sample Full Name']:
                                for wo in samples_dict[folder.name]:
                                    if cell.value in samples_dict[folder.name][wo]:
                                        sample_found = True
                            if cell.column_id == col_ids['Work Order ID']:
                                if cell.value in samples_dict[folder.name]:
                                    wo_found = True

                        if sample_found and wo_found:
                            status_cell = smartsheet.smartsheet.models.Cell()
                            date_cell = smartsheet.smartsheet.models.Cell()
                            if sample_status == 'qPCR drop-off':
                                date_cell.column_id = col_ids['qPCR drop off date']
                            elif sample_status == 'capture drop-off':
                                date_cell.column_id = col_ids['Capture drop off date']
                            date_cell.value = info['Date']
                            status_cell.column_id = col_ids['Current Production Status']
                            status_cell.value = sample_status


                            rows_to_update.append(smartsheet.smartsheet.models.Row())
                            rows_to_update[-1].id = row.id
                            rows_to_update[-1].cells.append(status_cell)
                            rows_to_update[-1].cells.append(date_cell)

                    # send update request to smartsheet
                    update_rows = smartsheet_cl.smart_sheet_client.Sheets.update_rows(sheet.id, rows_to_update)

    num_samples = 0
    for admin in samples_dict:
        for wo in samples_dict[admin]:
            num_samples += len(wo)
    return num_samples


def read_in_work_order(wo_file, info):

    admin_dict = {}

    with open(wo_file, 'r') as fin1:
        next(fin1)
        reader1 = csv.DictReader(fin1, delimiter='\t')
        next(reader1)

        for line in reader1:

            admin = line['Administration Project']
            wo = line['Work Order']
            bc = line['Barcode']

            if wo in info['Outgoing Queue Work Order']:
                if admin not in admin_dict:
                    admin_dict[admin] = {}
                if wo not in admin_dict[admin]:
                    admin_dict[admin][wo] = []
                if bc not in admin_dict[admin][wo]:
                    admin_dict[admin][wo].append(bc)

    return admin_dict


def clean_up_space():

    date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    if not os.path.exists('logs'):
        os.mkdir('logs')

    files = glob.glob('*')

    for file in files:
        if 'dilution_drop_off.tsv' == file:
            os.rename(file, '{d}_{f}'.format(d=date, f=file))
        elif 'logs' not in file:
            os.rename(file, 'logs/{d}_{f}'.format(d=date, f=file))


def update_admin_email(ssclient, facilitator):
    """

    :param ssclient: smrtqc object with Smartsheet client
    :param facilitator: string of facilitator name or email
    :return: facilitator as Smartsheet object if found
    """
    user_sheet = ssclient.get_object(252568824768388, 's')
    sheet_column_dict = ssclient.get_column_ids(user_sheet.id)
    found_user = False
    name = ''

    for row in user_sheet.rows:
        for cell in row.cells:
            if cell.column_id == sheet_column_dict['Name'] and facilitator == cell.value:
                found_user = True
            if cell.column_id == sheet_column_dict['User Name'] and facilitator == cell.value:
                found_user = True
            if found_user and cell.column_id == sheet_column_dict['Email Address']:
                return cell.value

    return facilitator + '@wustl.edu'


def main():

    # Set up Smartsheet client using smrtqc package and API key set as an environment variable
    api_key = os.environ.get('SMRT_API')
    ssobj = smrtqc.SmartQC(api_key)

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', help='Name of barcodes file Usage "-f <input-file>"', type=str)
    parser.add_argument('-q', help='Updates given barcodes as qpcr and generates dropoff in qPCR dropoff in PCC', action='store_true')
    parser.add_argument('-c', help='Updates given barcodes as capture and generates dropoff in Capture dropoff in PCC', action='store_true')
    parser.add_argument('-dev', help='Used for development and testing purposes', action='store_true')

    args = parser.parse_args()

    # Check for given file or request input
    if args.f:
        file = args.f
        if not os.path.exists(args.f):
            exit('{} not found!'.format(args.f))

    # Request input if not file flag
    else:

        print('No file flag found! \nWould you like to add barcode through terminal(y/n)? ', end='')
        ok = False
        while not ok:
            cin = input()
            if cin == 'y':
                file = get_bc_input()
                ok = True
            elif cin == 'n':
                print('-f is a required field, see smartflow usage using -h')
                exit('No barcodes entered, exiting.')
            else:
                print('Please enter either "y" or "n"')
                print('No file flag found! Would you like to add barcode through terminal(y/n)? ', end='')

    shutil.copyfile(file, ssobj.get_working_directory('qpcr') + '/' + file)

    orig_dir = os.getcwd()
    # Move to qPCR working directory
    os.chdir(ssobj.get_working_directory('qpcr'))

    # Set capture or qpcr
    if args.q:
        status = 'qPCR drop-off'
    elif args.c:
        status = 'capture drop-off'
    else:
        exit('Must specify qPCR(-qpcr) or Capture(-capture)')

    # Get dilution drop off and work order sheets via lims commands
    send_get_sheets(file=file)

    # Parse barcode and work order info from lims queries
    sheet_info = get_info('dilution_drop_off.tsv', 'work_order.tsv')

    # update sample statuses in MSS sheets
    num_samples = update_sample_statuses('dilution_drop_off.tsv', sheet_info, ssobj, status)

    # Update the PCC and load the drop off as row attachment
    load_dropoff_into_smartsheet('dilution_drop_off.tsv', 'work_order.tsv', sheet_info, ssobj, args, num_samples)

    # Move file to appropriate folders
    clean_up_space()

    # Return to original directory
    os.chdir(orig_dir)


if __name__ == '__main__':
    main()
