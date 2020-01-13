import sys
import smartsheet


class SmartQC:

    def __init__(self, api_key):

        if api_key is None:
            sys.exit('Api key not found')

        self.smart_sheet_client = smartsheet.Smartsheet(api_key)
        self.smart_sheet_client.errors_as_exceptions(True)
        self.routing_dict = {'qpcr':'/gscmnt/gc2746/production/smartflow/production_files/library_core/qpcr_dropoff',
                             'capture': '/gscmnt/gc2746/production/smartflow/production_files/library_core/capture_dropoff',
                             'pb': '/gscmnt/gc2746/production/smartflow/production_files/library_core/plate_building',
                             'rb': '/gscmnt/gc2746/production/smartflow/production_files/resource_bank',
                             'bin': '/gscmnt/gc2746/production/smartflow/bin',
                             'wm': '/gscmnt/gc2746/production/smartflow/bin/workflow_modules'}

        self.dev_routing_dict = {'qpcr': '/gscmnt/gc2746/production/smartflow/dev/production_files/library_core/qpcr_dropoff',
                                 'capture': '/gscmnt/gc2746/production/smartflow/dev/production_files/library_core/capture_dropoff',
                                 'pb': '/gscmnt/gc2746/production/smartflow/dev/production_files/library_core/plate_building',
                                 'rb': '/gscmnt/gc2746/production/smartflow/dev/production_files/resource_bank',
                                 'bin': '/gscmnt/gc2746/production/smartflow/dev/bin',
                                 'wm': '/gscmnt/gc2746/production/smartflow/dev/bin/workflow_modules'}

    @staticmethod
    def lt_test(lt):
        if lt not in ['f', 'w', 'h']:
            sys.exit('{} not a valid location tag'.format(lt))

    def create_folder(self, new_folder_name, location_id, location_tag):

        if location_tag == 'f':
            return self.smart_sheet_client.Folders.create_folder_in_folder(str(location_id), new_folder_name)
        if location_tag == 'w':
            return self.smart_sheet_client.Workspaces.create_folder_in_workspace(str(location_id), new_folder_name)
        if location_tag == 'h':
            return self.smart_sheet_client.Home.create_folder(new_folder_name)

        self.lt_test(location_tag)

    def create_workspace_home(self, workspace_name):
        # create WRKSP command
        return self.smart_sheet_client.Workspaces.create_workspace(
            smartsheet.models.Workspace({'name': workspace_name}))

    def get_sheet_list(self, location_id, location_tag):
        # Read in all sheets for account
        if location_tag == 'a':
            return self.smart_sheet_client.Sheets.list_sheets(include="attachments,source,workspaces",
                                                              include_all=True).data

        if location_tag == 'f' or location_tag == 'w':
            return self.get_object(str(location_id), location_tag).sheets

        self.lt_test(location_tag)

    def get_folder_list(self, location_id, location_tag):

        if location_tag == 'f' or location_tag == 'w':
            return self.get_object(str(location_id), location_tag).folders

        if location_tag == 'a':
            return self.smart_sheet_client.Home.list_folders(include_all=True)

        self.lt_test(location_tag)

    def get_workspace_list(self):
        # list WRKSPs command
        return self.smart_sheet_client.Workspaces.list_workspaces(include_all=True).data

    def get_column_ids(self, sheet_id):
        data = {}
        sheet_columns = self.smart_sheet_client.Sheets.get_columns(sheet_id)
        for col in sheet_columns.data:
            data[col.title] = col.id
        return data

    def get_sheet_with_columns(self, sheet_id, column_list):
        return self.smart_sheet_client.Sheets.get_sheet(sheet_id, column_ids=column_list)

    def get_object(self, object_id, object_tag):

        if object_tag == 'f':
            return self.smart_sheet_client.Folders.get_folder(str(object_id))
        if object_tag == 'w':
            return self.smart_sheet_client.Workspaces.get_workspace(str(object_id))
        if object_tag == 's':
            return self.smart_sheet_client.Sheets.get_sheet(str(object_id))

        self.lt_test(object_tag)

    def get_workspace(self, tag):

        # return dev workspace
        if tag == 'dev':
            # TODO: Add dev space once importing is completed and we have spare sheets

            return KeyError

        # return standard space
        elif tag == 'std':

            for space in self.get_workspace_list():
                if space.name == 'Smartflow Production Workspace':
                    return self.get_object(space.id, 'w')
                else:
                    return KeyError

        elif tag == 'lib_core':
            for space in self.get_workspace_list():
                if space.name == 'Library Core Workspace':
                    return self.get_object(space.id, 'w')

    def get_working_directory(self, tag, dev=False):

        # TODO: Add locations for all other commands in smartflow

        if not dev:
            return self.routing_dict[tag]
        else:
            return self.dev_routing_dict[tag]