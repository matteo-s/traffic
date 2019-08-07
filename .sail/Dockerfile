# Use a predefined language base.
FROM codercom/ubuntu-dev-python3.7:latest


# Install some developer tooling to help out with system 
# and program monitoring.
#RUN sudo apt-get update -y && sudo apt-get install -y \
#    dstat \
#    wireshark

# Install setuptools to use with your python project.
#RUN pip install -U setuptools

# Add any environment vars you could want.
#ENV PATH $PATH:/my/additional/bins

# Add a shared dir for project data.
LABEL share.app_cache "~/app/cache:~/app/cache"

