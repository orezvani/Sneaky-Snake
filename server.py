class server:

    def __init__(username, hostaddress):
        self.user = username            # User to run the job on the server
        self.host = hostaddress         # Host Address of the server
        self.jpid = -1                  # Job PID
        self.jstr = ""                  # Job command string
        self.cpyd = False               # Files are copied
        self.cnkd = False               # Connected to the server


    """ This method returns the status of the server, 0 is not connected, +1 is connected but files not copied, +2 is waiting for a job, +3 is running a job """
    def get_status():
        return 0


    """ This method returns the status of the job running on the server, 0 is finished, +1 is running """
    def get_job_status():
        return 0;


    """ This method runs a job that is specified in the form of a command string """
    def run_job(job_command_string):
        return 0


    """ This method copies the files to the server """
    def copy_files(files):
        return 0;
