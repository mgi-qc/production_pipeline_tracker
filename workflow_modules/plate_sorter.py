#!/usr/bin/python3
__author__ = "Thomas Antonacci"


import smartsheet
import csv
import os
import glob
import datetime
import subprocess
import smrtqc


# Classes for plate building
class sample:
    """Represents a single instance of a sample in the production pipeline"""
    def __init__(self):

        # sample full name from sample_inventory
        self.name = ''
        # sample barcode from dilution drop off
        self.bc = ''
        # sample source barcode from drop off
        self.source_bc = ''
        # Freezer location from freezer loc file
        self.loc = ''
        # Sample work order from dilution drop off
        self.work_order = ''
        # Sample pipeline determined from the pipelines file and dilution drop off
        self.pipe = ''
        # Sample plate retrieved from the sample location at initialization
        self.plate = ''
        # Sample marked as FFPE or not (Boolean True/False)
        self.FFPE = ''


class plate:
    """Represents a single plate in the production pipeline"""
    def __init__(self):
        # Barcode of plate in lims
        self.name = ''
        # List of sample objects representing samples in the plate
        self.samples = []
        # List of work orders of samples in the plate
        self.wo = []
        # Outgoing pipeline of the samples in the plate (Outgoing only!)
        self.pipe = ''

    """Most of these functions are not used, it is possible they could be utiilized to streamline some of this code"""
    def set_name(self, name):
        self.name = name
        return None

    def set_samples(self, list_of_samples):
        self.samples = list_of_samples
        return None

    def set_work_orders(self, list_of_wos):
        self.wo = list_of_wos
        return None

    def set_pipe(self, pipe):
        if self.pipe is None:
            self.pipe = pipe
            return None
        elif self.pipe == pipe:
            return
        else:
            print('Pipe already set for Plate {}!'.format(self.name))
            return None

    def add_sample(self, sampl):
        if type(sampl) is list:
            for samp in sampl:
                self.samples.append(samp)
                return None
        else:
            self.samples.append(sampl)
            return None

    def add_wo(self, workorder):
        if workorder in self.wo:
            return
        else:
            self.wo.append(workorder)
            return


class work_order:

    def __init__(self):
        # Work order id number
        self.name = ''
        # List of plates that have samples from this work order
        self.in_plates = []
        # List of samples in this work order
        self.samples = []
        # Pipeling for this work order
        self.pipe = None

    def set_name(self, name):
        self.name = name
        return

    def add_in_plate(self, plt):
        self.in_plates.append(plt)
        return None

    def add_sample(self, sampl):
        # Adds sample(s)

        if type(sampl) is list:
            for samp in sampl:
                self.samples.append(samp)
                return None
        else:
            self.samples.append(sampl)
            return None


class Ffd:
    """
    This class will take the sorted set boxes(decreasing in size) of plate objects and use the first fit bin packing algorithm to return a list of lists of what plates to pack together
    Must set capacity of the plates and the boxes list when initializing the object
    """

    def __init__(self, capacity, boxes):
        self.capacity = capacity
        self.boxes = boxes

    def add_to_bins(self, box, bins):
        """Recursive implementation of FFD algorithm"""

        for b in bins:
            bin_value = 0
            for t in b:
                bin_value += len(t.samples)
            if bin_value + len(box.samples) < self.capacity:
                b.append(box)
                return bins
        bins.append([])
        self.add_to_bins(box, bins)
        return bins

    def ffd(self):
        """Initializes the recursive FFD algorithm"""
        bins = []
        for item in self.boxes:
            bins = self.add_to_bins(item, bins)
        return bins


def main():
    """
    TODO: Refactor code into packages/modules to lessen the amount of code in this file
    TODO: Better output
    TODO: Log failures
    """
    try:
        # Change to correct directory
        orig_dir = os.getcwd()
        os.chdir('/gscmnt/gc2746/production/smartflow/production_files/library_core/plate_building')

        # Get Smaetsheet client
        ss_client = smrtqc.SmartQC(api_key=os.environ.get('SMRT_API'))

        # Import Pipeline file
        pipe_dict = get_pipelines()

        # Import FFPE file
        FFPE_dict = get_FFPE_names()

        # get BCS either from Jen
        barcodes = get_bcs()

        # Queries Dropoff, Freezer Loc, Samples sheet(Possibly)
        run_queries(barcodes)

        # Make sample objects
        sample_master_list = build_sample_objects(pipelines=pipe_dict,bcs=barcodes)

        # Sort samples to current plates
        master_current_plates = sort_to_current_plates(sample_master_list)

        # Mark Samples as FFPE
        mark_FFPE_samples(sample_master_list, FFPE_dict)

        # Sort samples to WOs (Not used...)
        # wo_master_list = sort_to_work_order(sample_master_list)

        # Sort samples to outgoing plates
        outgoing_plates = sort_to_outgoing_plates(plate_list=master_current_plates)

        # Update SmartSheets
        update_smart_sheets\
            (sample_master_list, outgoing_plates, ss_client)
    finally:

        # Update FFPE file
        update_FFPE_file(FFPE_dict)

        # Update Pipeline file
        update_pipeline_file(pipe_dict)

        # Cleanup
        clean_up_workspace()

        # move back to original dir
        os.chdir(orig_dir)


def get_pipelines():
    """

    :return:
    """

    # Read in pipeline file and load pipeline dict
    pipelines = {}

    while True:
        try:
            with open('config/pipelines_file.csv', 'r+') as pipe_f:

                pipe_read = csv.DictReader(pipe_f, delimiter=',')

                for line in pipe_read:
                    if line['Pipeline'] == 'e':
                        pipelines[line['Name']] = 'e'
                    elif line['Pipeline'] == 'w':
                        pipelines[line['Name']] = 'w'
                    elif line['Pipeline'] == 'o':
                        pipelines[line['Name']] = 'o'
            break
        except FileNotFoundError:
            # make Pipelines file if not found
            print('Pipeline File not found\nBuilding pipelines_file.csv...')
            with open('config/pipelines_file.csv', 'w') as pipes:
                pipes.write('Name,Pipeline')

    return pipelines


def get_FFPE_names():
    """

    :return:
    """

    # Read in FFPE file and load FFPE dict
    FFPE_dict = {}

    while True:
        try:
            with open('config/FFPE_names.csv', 'r+') as FFPE_f:

                FFPE_read = csv.DictReader(FFPE_f, delimiter=',')

                for line in FFPE_read:
                    if line['FFPE'] is 't':
                        FFPE_dict[line['Name']] = True
                    else:
                        FFPE_dict[line['Name']] = False
            break
        except FileNotFoundError:
            # Make FFPE file if not found
            print('FFPE File not found\nBuilding FFPE_names.csv...')
            with open('config/FFPE_names.csv', 'w') as FFPE_f:
                FFPE_f.write('Name,FFPE')

    return FFPE_dict


def get_bcs():
    """

    :return:
    """

    # Get barcodes from Jen pasting in terminal
    print('---------------')
    print('Please paste in barcodes from Dilution Drop Off (Enter "return q return" when finished): ')

    bcs = []
    while True:
        bc_line = input()
        if bc_line != 'q':
            bcs.append(bc_line.strip())
        else:
            break
    print('---------------')

    return bcs


def run_queries(bcs):
    """

    :param bcs:
    :return:
    """

    # bcs : list of barcodes

    print('Running LIMS queries')
    bc_string = ','.join(bcs)

    # Temp print statements until cluster testing
    # print(' '.join(['barcode_info', '-r', 'dilution_drop_off', '-bc',bc_string, '--format', 'tsv']))
    # print(' '.join(['barcode_info', '-r', 'freezer_loc', '-bc', bc_string, '--format', 'tsv']))
    # print(' '.join())

    print(' - Getting dilution drop off')
    subprocess.run(['barcode_info', '-r', 'dilution_drop_off', '-bc', bc_string, '--format', 'tsv'])

    print(' - Getting freezer locations')
    subprocess.run(['barcode_info', '-r', 'freezer_loc', '-bc', bc_string, '--format', 'tsv'])

    if not os.path.isfile('dilution_drop_off.tsv'):
        exit('dilution_drop_off not generated: Please save any error messages and contact antonacci.t.j or ltrani')
    if not os.path.isfile('freezer_loc.tsv'):
        exit('freezer_loc file not generated: Please save any error messages and contact antonacci.t.j or ltrani')
    print('---------------')


def build_sample_objects(pipelines, bcs):
    """

    :param pipelines:
    :param bcs:
    :return:
    """
    print('- Building sample objects')

    master_sample_list = []

    with open('dilution_drop_off.tsv', 'r') as fin1, open('freezer_loc.tsv', 'r') as fin2:

        # Open dil drop report and freezer loc file and prepare readers
        next(fin1)
        next(fin2)

        reader1 = csv.DictReader(fin1, delimiter='\t')
        reader2 = csv.DictReader(fin2, delimiter='\t')

        next(reader1)
        next(reader2)

        # get freezer locations for barcodes

        bc_freezer_loc = get_bc_freezer_loc(barcodes=bcs, fl_reader=reader2)

        # Build sample objects using bc and wo from dil drop and sample name from query

        for line in reader1:
            if line['Barcode'] in bcs:
                # get pipeline
                if line['Outgoing Queue Work Order Pipeline'] in pipelines:
                    pipe = pipelines[line['Outgoing Queue Work Order Pipeline']]
                else:
                    print('"e" for Exome\n'
                          '"w" for Whole Genome\n'
                          '"o" for other pipelines\n'
                          'Please enter the appropriate pipeline for {}: '.format(line['Outgoing Queue Work Order Pipeline']))

                    while True:
                        pin = input()
                        if pin in ['e','w','o']:
                            pipe = pin
                            pipelines[line['Outgoing Queue Work Order Pipeline']] = pin
                            break
                        else:
                            print('Please enter "e","w", or "o": ')
                    print('---------------')
                # name=bc_samp[line['Barcode']], work_order=line['Outgoing Queue Work Order'], loc=bc_freezer_loc[line['Barcode']], bc=line['Barcode'], pipe=pipe,FFPE=False
                current_sample = sample()
                current_sample.work_order = line['Outgoing Queue Work Order']
                current_sample.pipe = pipe
                current_sample.source_bc = line['Source BC']
                current_sample.loc = bc_freezer_loc[line['Barcode']]
                try:
                    current_sample.plate = current_sample.loc.split(' ')[-3]
                except IndexError:
                    print('Freezer Location not found, please ensure samples are checked into freezer before running.')
                    exit(' - Exiting Plate Builder')
                current_sample.bc = line['Barcode']
                master_sample_list.append(current_sample)

    print('- Getting sample names')
    get_samp_names(master_sample_list)

    return master_sample_list


def get_samp_names(samples):
    """

    :param samples:
    :return:
    """
    bcs = []
    for samp in samples:
        bcs.append(samp.bc)
    FNULL = open(os.devnull, 'w')
    subprocess.run(['limfo', 'bar', '-bc', ','.join(bcs), '--report', 'sample_inventory', '--format', 'tsv'], stdout=FNULL, stderr=subprocess.STDOUT)

    with open('sample_inventory.tsv') as inv_file:
        next(inv_file)

        reader = csv.DictReader(inv_file, delimiter='\t')
        next(reader)

        for line in reader:
            samples[int(line['#'])-1].name = line['DNA']
        count = len(samples)

        print('   {} samples found.'.format(count))


def get_bc_freezer_loc(barcodes, fl_reader):
    """

    :param barcodes:
    :param fl_reader:
    :return:
    """

    bc_freezer_loc = {}

    for line in fl_reader:
        for bc in barcodes:
            if bc in line.values():
                bc_freezer_loc[bc] = line['Freezer_Loc']

    return bc_freezer_loc


def sort_to_current_plates(sample_list):
    """

    :param sample_list:
    :return:
    """
    master_plate = []

    for samp in sample_list:
        found = False

        for plt in master_plate:
            if samp.plate == plt.name:
                plt.samples.append(samp)
                if samp.work_order not in plt.wo:
                    plt.wo.append(samp.work_order)
                samp.plate = plt
                found = True

        if not found:
            master_plate.append(plate())
            master_plate[-1].name = samp.plate
            master_plate[-1].wo.append(samp.work_order)
            master_plate[-1].samples.append(samp)
            samp.plate = master_plate[-1]

    return master_plate


def mark_FFPE_samples(sample_list, FFPE_dict):
    """
    get wo inventory from query and use Tissue Type to mark FFPE samples

    :param sample_list:
    :param FFPE_dict:
    :return:
    """

    wos = []
    for samp in sample_list:
        if samp.work_order not in wos:
            subprocess.run(['wo_info', '-r', 'inventory', '-woid', samp.work_order, '--format', 'tsv'])
            read_in_wo_inventory('inventory.tsv', FFPE_dict, sample_list, samp.work_order)
            wos.append(samp.work_order)


def read_in_wo_inventory(file_name, FFPE_dict, sample_list, wo):
    """

    :param file_name:
    :param FFPE_dict:
    :param sample_list:
    :param wo:
    :return:
    """

    with open(file_name, 'r') as inv_file:

        found = False
        while not found:
            line = inv_file.readline().strip()
            if line == 'Inventory':
                found = True

        inv_reader = csv.DictReader(inv_file, delimiter='\t')
        next(inv_reader)
        for line in inv_reader:
            if line['Tissue Type'] not in FFPE_dict:
                # prompt for FFPE or not
                print('Are "{}" samples FFPE (y/n)? '.format(line['Tissue Type']))
                while True:
                    cin = input()
                    if cin == 'y':
                        FFPE_dict[line['Tissue Type']] = True
                        break
                    elif cin == 'n':
                        FFPE_dict[line['Tissue Type']] = False
                        break
                    else:
                        print('Please enter "y" or "n": ')
                print('---------------')
            for samp in sample_list:
                if samp.name == line['Content_Desc']:
                    samp.FFPE = FFPE_dict[line['Tissue Type']]
                    break
    os.rename(file_name, wo + '_' + file_name)


def sort_to_work_order(sample_list):
    """

    :param sample_list:
    :return:
    """

    wo_list = []

    for samp in sample_list:
        found = False
        for wo in wo_list:
            if samp.work_order == wo.name:
                wo.samples.append(samp)

                if samp.plate not in wo.in_plates:
                    wo.in_plates.append(samp.plate)
                found = True

        if not found:
            wo_list.append(work_order())
            wo_list[-1].name = samp.work_order
            wo_list[-1].samples.append(samp)
            wo_list[-1].pipe = samp.pipe
            wo_list[-1].in_plates.append(samp.plate)

    return wo_list


def sort_to_outgoing_plates(plate_list):
    """

    :param plate_list:
    :return:
    """

    print('- Sorting sample to plates')
    print('---------------')

    # ini lists
    outgoing_plates = []

    # get 96 plates with 1 wo
    presort_bins = get_96_plates(plate_list, outgoing_plates)

    # make bin lists
    bins = bin_presort(presort_bins, outgoing_plates)

    # Break up bins by pipeline
    pipeline_bin_dict = put_bin_in_pipe(bins)

    # Bin sorting for each pipeline
    combined_plates = []
    for pipe in pipeline_bin_dict:

        pipeline_bin_dict[pipe] = order_bins(pipeline_bin_dict[pipe])

        packed_plates = Ffd(96, pipeline_bin_dict[pipe])

        packed_plates = packed_plates.ffd()

        # Translate bins to outgoing plates
        for plt in packed_plates:
            combined_plates.append(plate())
            for box in plt:
                combined_plates[-1].samples.extend(box.samples)

    # Add combined plates to outgoing
    if len(combined_plates) != 0:
        outgoing_plates.extend(combined_plates)

    # rename plates based on work orders and FFPE present

    # Pass list of outgoing plates back to main
    return outgoing_plates


def get_96_plates(plates, outgoing_plates):
    """

    :param plates:
    :param outgoing_plates:
    :return:
    """
    sorting_plates = []

    for plt in plates:
        if len(plt.samples) == 96 and len(plt.wo) == 1:
            outgoing_plates.append(plt)
        else:
            sorting_plates.append(plt)

    return sorting_plates


def bin_presort(plates, outgoing_plates):
    """

    :param plates:
    :param outgoing_plates:
    :return:
    """
    boxes1 = []
    boxes2 = []
    boxes3 = []

    # Sort samples to boxes with 1 wo and 1 plate per
    for plate_in in plates:
        for samp in plate_in.samples:
            found = False
            for box in boxes1:
                if box.name == samp.plate and samp.work_order in box.wo:
                    box.samples.append(samp)
                    found = True

            if not found:
                boxes1.append(plate())
                boxes1[-1].samples.append(samp)
                boxes1[-1].wo.append(samp.work_order)
                boxes1[-1].name = samp.plate

    # Combine plates < 96 that also have same wo; Put plates with 96 into outgoing
    for box1 in boxes1:
        found = False
        for box2 in boxes2:
            if box1.wo[0] == box2.wo[0]:
                box2.samples.extend(box1.samples)
                found = True

        if not found:
            boxes2.append(plate())
            boxes2[-1].samples = box1.samples
            boxes2[-1].wo = box1.wo
            boxes2[-1].name = box1.name

    # if new boxes > 96, take extra and make new box
    for box in boxes2:
        if len(box.samples) == 96:
            outgoing_plates.append(box)
        elif len(box.samples) < 96:
            boxes3.append(box)
        elif len(box.samples) > 96:
            boxes3.append(plate())
            boxes3[-1].wo = box.wo
            boxes3[-1].name = box.name
            for samp in box.samples:
                if len(boxes3[-1].samples) < 96:
                    boxes3[-1].samples.append(samp)
                else:
                    boxes3.append(plate())
                    boxes3[-1].wo = box.wo
                    boxes3[-1].name = box.name
                    boxes3[-1].samples.append(samp)

    return boxes3


def put_bin_in_pipe(bins):

    pipe_bin_dict = {}

    for box in bins:
        for samp in box.samples:
            if box.pipe != samp.pipe:
                box.pipe = samp.pipe

    for box in bins:
        if box.pipe not in pipe_bin_dict:
            pipe_bin_dict[box.pipe] = [box]
        else:
            pipe_bin_dict[box.pipe].append(box)

    return pipe_bin_dict


def order_bins(bins):
    """

    :param bins: list of plate objects
    :return: list of plate objects sorted greatest to least
    """
    sorted_bins = []

    # Insertion sort for boxes
    for box1 in bins:
        placed = False

        for box2 in sorted_bins:

            if len(box1.samples) > len(box2.samples):
                sorted_bins.insert(sorted_bins.index(box2), box1)
                placed = True
                break
        if not placed:
            sorted_bins.append(box1)

    return sorted_bins


def update_smart_sheets(sample_list, outgoing_plates, ss_client):
    """
    TODO: Make lib core update into own function
    TODO: Remove hard coded folder

    :param sample_list: master list of sample objects
    :param outgoing_plates: list of all outgoing plates made using ffd
    :param ss_client: smrtqc object containing Smartsheet client
    :return: None
    """

    print('Updating Smartsheet:')
    print('- Loading plate sheets into Library Core')
    workspaces = ss_client.get_workspace_list()

    # TODO: Make Library Core Space/Folder somewhere that makes sense
    for workspace in workspaces:
        if workspace.name == 'Library Core Workspace':
            lib_workspace = ss_client.get_object(workspace.id, 'w')

    for folder in lib_workspace.folders:
        if folder.name == 'Plate Sheets':
            plate_folder = ss_client.get_object(folder.id, 'f')

    for sheet in lib_workspace.sheets:
        if sheet.name == 'plate_assignment_sheet':
            assgn_sheet = sheet

    assgn_sheet = ss_client.get_object(assgn_sheet.id, 's')
    assgn_sheet_col_ids = ss_client.get_column_ids(assgn_sheet.id)
    num_rows = len(assgn_sheet.rows) + 1

    # Make plate sheets using dilution drop off, freezer loc, master sample list,
    for plt in outgoing_plates:

        file = build_plate_sheet(plt, sample_list)

        imported_sheet = ss_client.smart_sheet_client.Folders.import_csv_sheet(
            plate_folder.id,  # folder_id
            file,
            file,  # sheet_name
            header_row_index=0
        ).data

        imported_sheet = ss_client.get_object(imported_sheet.id, 's')

        # get FFPE tag, Work Orders, and number of samples
        wos = []
        FFPE = False
        count = 0
        for samp in plt.samples:
            if samp.FFPE is True:
                FFPE = True
            if samp.work_order not in wos:
                wos.append(samp.work_order)
            count += 1

        new_row = smartsheet.smartsheet.models.Row()
        new_row.to_bottom = True

        new_row.cells.append({'column_id': assgn_sheet_col_ids['Plate File Name'], 'value': file})
        new_row.cells.append({'column_id': assgn_sheet_col_ids['Work Orders'], 'value': ','.join(wos)})
        new_row.cells.append({'column_id': assgn_sheet_col_ids['Link to Plate Sheet'], 'value': file, 'hyperlink': {"sheetId": imported_sheet.id}})
        new_row.cells.append({'column_id': assgn_sheet_col_ids['No. of Samples'], 'value': count})
        new_row.cells.append({'column_id': assgn_sheet_col_ids['FFPE Flag'], 'value': FFPE})
        new_row.cells.append({'column_id': assgn_sheet_col_ids['Task'], 'formula': '=IF([Fragmentation Complete]{} = 1, IF([Lib Construction Complete]{} = 1, IF([QC Complete]{} = 1, "Complete", "QC"), "Lib Construction"), "Fragmentation")'.format(num_rows, num_rows, num_rows)})

        response = ss_client.smart_sheet_client.Sheets.add_rows(assgn_sheet.id, [new_row])
        response = response.data

        num_rows += 1


    # Build smartsheet rows for import into plate_assignment_sheet
    # Import sheets and rows into Smartsheet

    print('- Updating PCC')
    # Update Production Communication Center (return dict of admin proj, wo, and samples)
    admin_wo_samp_dict = update_PCC(sample_list, ss_client)

    print('- Updating MSS (This may take some time...)')
    # Update Master Sample Sheets
    update_MSS_sheets(admin_wo_samp_dict, ss_client)
    print('---------------')


def update_PCC(sample_list, ss_client):
    """

    :param sample_list: master list of sample objects
    :param ss_client: smrtqc object with Smartsheet client
    :return: dictionary of dictionaries with hierarchy admin proj : wo : sample name
    """

    admin_wo_samp_dict = {}

    row_dict = {'do_name': '', 'wo': [], 'no_items': len(sample_list), 'fails': '', 'admin_proj': [], 'facilitator': [], 'date': datetime.datetime.now().strftime('%m%d%y')}

    # get wos and admin projs
    for samp in sample_list:

        if samp.work_order not in row_dict['wo']:
            row_dict['wo'].append(samp.work_order)


    for wo in row_dict['wo']:
        with open('{}_inventory.tsv'.format(wo)) as fin:
            found = False
            while not found:
                line = fin.readline().strip('\n')
                if 'Administration Project:' in line:
                    admin_proj = line.split('\t')[1].strip('"')
                    if admin_proj not in row_dict['admin_proj']:
                        row_dict['admin_proj'].append(admin_proj)

                    if admin_proj not in admin_wo_samp_dict:
                        admin_wo_samp_dict[admin_proj] = {wo: []}
                    elif wo in admin_wo_samp_dict[admin_proj]:
                        admin_wo_samp_dict[admin_proj][wo] = []
                    found = True

    with open('dilution_drop_off.tsv') as fin:
        row_dict['do_name'] = fin.readline().strip().strip('"')

        fin_reader = csv.DictReader(fin, delimiter='\t')
        next(fin_reader)

        for row in fin_reader:
            if row['Outgoing Queue Work Order Facilitator'] not in row_dict['facilitator']:
                row_dict['facilitator'].append(row['Outgoing Queue Work Order Facilitator'])


    # get PCC sheet
    prod_space = ''
    for wrksp in ss_client.get_workspace_list():
        if wrksp.name == 'Smartflow Production Workspace':
            prod_space = wrksp
    if prod_space == '':
        exit('Production Space not found!')

    for sheet in ss_client.get_sheet_list(prod_space.id, 'w'):
        if sheet.name == 'Production Communications Center':
            prod_comm_sheet = sheet
    if not prod_comm_sheet:
        print('Production Communication Center sheet not found!')
        print(' - Cannot update Production Communication Center.')
    else:
        prod_comm_sheet = ss_client.get_object(prod_comm_sheet.id, 's')
        col_ids = ss_client.get_column_ids(prod_comm_sheet.id)

        for row in prod_comm_sheet.rows:
            for cell in row.cells:
                if cell.value == 'LC dilution drop-off':
                    lcddo_row = row

        new_row = smartsheet.smartsheet.models.Row()
        new_row.parent_id = lcddo_row.id
        new_row.cells.append({'column_id': col_ids['Reporting Instance'], 'value': row_dict['do_name']})
        new_row.cells.append({'column_id': col_ids['Sequencing Work Order'], 'value': ','.join(row_dict['wo'])})
        new_row.cells.append({'column_id': col_ids['Items'], 'value': row_dict['no_items']})
        new_row.cells.append({'column_id': col_ids['Admin Project'], 'value': ','.join(row_dict['admin_proj'])})
        new_row.cells.append({'column_id': col_ids['Event Date'], 'value': datetime.datetime.now().strftime("%Y-%m-%d")})

        facil_list = []

        for contact in row_dict['facilitator']:
            facil_list.append(update_admin_email(ss_client, contact))

        new_row.cells.append({'column_id': col_ids['Facilitator'], 'object_value': ', '.join(facil_list)})

        new_row_response = ss_client.smart_sheet_client.Sheets.add_rows(prod_comm_sheet.id, [new_row]).data[0]

        attached_file = ss_client.smart_sheet_client.Attachments.attach_file_to_row(prod_comm_sheet.id, new_row_response.id, ('dilution_drop_off.tsv', open('dilution_drop_off.tsv'), 'rb'))

    for samp in sample_list:
        for admin_proj in admin_wo_samp_dict:
            for wo in admin_wo_samp_dict[admin_proj]:
                if wo == samp.work_order:
                    admin_wo_samp_dict[admin_proj][wo].append(samp.name)

    return admin_wo_samp_dict


def build_plate_sheet(out_plate, sample_list):
    """

    :param out_plate: plate object
    :param sample_list: master list of sample objects
    :return: Name of plate sheet made using wo number of samples and pipeline (as well as FFPE if present)
    """
    # build file name:
    wos = []
    pipe = ''
    date = datetime.datetime.now().strftime('%m%d%y')
    num_samples = len(out_plate.samples)
    count = 1

    # Get/check pipeline for samples and set plate pipe
    for samp in out_plate.samples:
        if out_plate.pipe == '':
            out_plate.pipe = samp.pipe
        elif out_plate.pipe != samp.pipe:
            exit('Error: Pipeline mismatch in plate')

        if samp.work_order not in wos:
            wos.append(samp.work_order)
    if out_plate.pipe == 'w':
        pipe = "WGS"
    elif out_plate.pipe == 'e':
        pipe = "Exome"
    elif out_plate.pipe == 'o':
        pipe = "Other"

    # Check for files name in dir
    found = True
    while found:
        sheet_name = '_'.join([str(num_samples), 'Frag_Plate', str(count), pipe, date]) + '.csv'
        if os.path.exists(sheet_name):
            count += 1
        else:
            break

    for wo in wos:
        with open('dilution_drop_off.tsv', 'r') as ddo, open('{}_inventory.tsv'.format(wo), 'r') as inv_f, open(sheet_name, 'w') as fout:
            next(ddo)
            ddo_reader = csv.DictReader(ddo, delimiter='\t')
            next(ddo_reader)
            headers = ddo_reader.fieldnames
            headers.append('Freezer_loc')

            fout_writer = csv.DictWriter(fout, fieldnames=headers, delimiter=',')
            fout_writer.writeheader()

            for line in ddo_reader:
                for samp in out_plate.samples:
                    if samp.bc == line['Barcode']:
                        line['Freezer_loc'] = samp.loc
                        fout_writer.writerow(line)

    return sheet_name


def update_MSS_sheets(admin_wo_samp_dict, ss_client):

    """

    :param admin_wo_samp_dict: Dictopnary with hierarchy of admin project : work order : sample name
    :param ss_client: smrtqc object with smartsheet client
    :return: No return value
    """

    ss_headers = ['Work Order ID', 'Sample Full Name']
    updated_samples = {}
    missing_samples = {}
    num_samples = 0

    for admin in admin_wo_samp_dict:
        for wo in admin_wo_samp_dict[admin]:
            num_samples += len(admin_wo_samp_dict[admin][wo])

    for space in ss_client.get_workspace_list():
        if space.name == 'Smartflow Production Workspace':
            prod_space = space

    for admin in admin_wo_samp_dict:
        for folder in ss_client.get_folder_list(prod_space.id, 'w'):
            if folder.name == 'Admin Projects':
                admin_folder = ss_client.get_object(folder.id, 'f')

        for folder in admin_folder.id:
            if folder.name == 'Active Projects':
                active_folder = ss_client.get_object(folder.id, 'f')

        for folder in active_folder.folders:
            if admin == folder.name:
                MSS_folder = folder

        for sheet in ss_client.get_sheet_list(MSS_folder.id, 'f'):
            if 'MSS' in sheet.name:
                rows_to_update = []

                col_ids = ss_client.get_column_ids(sheet.id)
                ids = []
                for header in col_ids:
                    if header in ss_headers:
                        ids.append(col_ids[header])

                mss_sheet = ss_client.get_sheet_with_columns(sheet.id, ids)

                for row in mss_sheet.rows:
                    sample_found = False

                    for cell1 in row.cells:
                        if cell1.column_id == col_ids['Work Order ID']:
                            for wo in admin_wo_samp_dict[admin]:
                                if wo not in updated_samples:
                                    updated_samples[wo] = []
                                if cell1.value == wo:
                                    for cell2 in row.cells:
                                        if cell2.column_id == col_ids['Sample Full Name']:
                                            for samp in admin_wo_samp_dict[admin][wo]:
                                                if cell2.value == samp:

                                                    sample_found = True
                                                    updated_samples[wo].append(samp)

                    if sample_found:
                        status_cell = smartsheet.smartsheet.models.Cell()
                        status_cell.column_id = col_ids['Current Production Status']
                        status_cell.value = 'Library Core'

                        date_cell = smartsheet.smartsheet.models.Cell()
                        date_cell.column_id = col_ids['Lib Core Start Date']
                        date_cell.value = datetime.datetime.now().strftime("%Y-%m-%d")

                        rows_to_update.append(smartsheet.smartsheet.models.Row())
                        rows_to_update[-1].id = row.id
                        rows_to_update[-1].cells.append(status_cell)
                        rows_to_update[-1].cells.append(date_cell)

                update_rows = ss_client.smart_sheet_client.Sheets.update_rows(sheet.id, rows_to_update)

    for admin in admin_wo_samp_dict:
        for wo in admin_wo_samp_dict[admin]:
            for samp in admin_wo_samp_dict[admin][wo]:
                if samp not in updated_samples[wo]:
                    if wo not in missing_samples:
                        missing_samples[wo] = [samp]
                    else:
                        missing_samples[wo].append(samp)

    if len(missing_samples) > 0:
        print('These samples were not found in any MSS sheets (Please contact mgi-qc):')
        for wo in missing_samples:
            if len(missing_samples[wo]) > 0:
                for samp in missing_samples[wo]:
                    print("{wo} : {samp}".format(wo=wo, samp=samp))


def update_admin_email(ssclient, facilitator):
    """

    :param ssclient: smrtqc object with Smartsheet client
    :param facilitator: string of facilitator name or email
    :return: facilitator as Smartsheet object if found
    """
    user_sheet = ssclient.get_object(252568824768388, 's')
    sheet_column_dict = ssclient.get_column_ids(user_sheet.id)
    found_user = False

    for row in user_sheet.rows:
        for cell in row.cells:
            if cell.column_id == sheet_column_dict['Name'] and facilitator == cell.value:
                found_user = True
            if cell.column_id == sheet_column_dict['User Name'] and facilitator == cell.value:
                found_user = True
            if found_user and cell.column_id == sheet_column_dict['Email Address']:
                return cell.value

    return facilitator


def update_pipeline_file(pipelines):
    """

    :param pipelines:
    :return: None
    """

    with open('config/pipelines_file.csv', 'w') as pipe_f:
        pipe_dict_write = csv.DictWriter(pipe_f, delimiter=',', fieldnames=['Name', 'Pipeline'])
        pipe_dict_write.writeheader()

        line = {'Name': '', 'Pipeline': ''}
        for item in pipelines:
            line['Name'] = item
            line['Pipeline'] = pipelines[item]
            pipe_dict_write.writerow(line)


def update_FFPE_file(FFPE_dict):
    """

    :param FFPE_dict:
    :return: None
    """

    with open('config/FFPE_names.csv', 'w') as FFPE_f:
        FFPE_dict_write = csv.DictWriter(FFPE_f, delimiter=',', fieldnames=['Name', 'FFPE'])
        FFPE_dict_write.writeheader()

        line = {'Name': '', 'FFPE': ''}
        for item in FFPE_dict:
            line['Name'] = item
            if FFPE_dict[item] is True:
                line['FFPE'] = 't'
            else:
                line['FFPE'] = 'f'
            FFPE_dict_write.writerow(line)


def clean_up_workspace():
    """
    :return: None
    """
    if not os.path.exists('logs'):
        os.mkdir('logs')
    if not os.path.exists('plate_files'):
        os.mkdir('plate_files')

    date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    cleanup_list = ['freezer_loc.tsv']
    cleanup_list.extend(glob.glob('*inventory.tsv'))
    plate_file_list = glob.glob('*_Frag_Plate*')

    for file in cleanup_list:
        os.rename(file, 'logs/{d}_{f}'.format(d=date, f=file))

    for file in plate_file_list:
        os.rename(file, 'plate_files/' + file)

    os.rename('dilution_drop_off.tsv', '{}_dilution_drop_off.tsv'.format(date))


if __name__ == '__main__':
    main()
