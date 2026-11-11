"""When you instantiate this profile, the repository is cloned to all of the nodes in your experiment, to `/local/repository`. 

This particular profile is used for benchmarking different data streaming process with varying degree of configurations.
This profile creates main node and dsp benchamrking nodes based on user input. 
It can be instantiated with different cluster hardware types which are most available;

Instructions:
Wait for the profile instance to start, then click on the node in the topology and choose the `shell` menu item. 
"""

# Import the Portal object.
import geni.portal as portal

# Import the ProtoGENI library.
import geni.rspec.pg as pg


# Create a portal context.
pc = portal.Context()


# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()




#
# This is a typical list of hardware types.
#
hardware_list=[
    'm400','xl170','d6515','r650','rs630','c220g5','c6525-25g','c6525-100g','c6320','d710','m510','rs620','c6420','c8220','c8220x','dss7500'
]


# Take User input for the number of nodes in the DSP benchmarking setup
pc.defineParameter(name="dspNodeCount", 
                   description="Number of nodes in the DSP benchmarking setup",
                   typ=portal.ParameterType.INTEGER, 
                   defaultValue=6)

# Take user input for the hardware type. The types might not be available.
# The list contains hardware types available in large quantity to prevent any failures.
pc.defineParameter(name="dspHardwareType", 
                   description="Select Hardware type",
                   typ=portal.ParameterType.STRING,
                   defaultValue=hardware_list[0],
                   legalValues=hardware_list,
                   longDescription="Select a hardware type.Consider some hardware types might not be available.")

# Retrieve the values the user specifies during instantiation
params = pc.bindParameters()

# Check parameter validity
if params.dspNodeCount < 2:
    pc.reportError(portal.ParameterError("You must choose at least 2 node", ("dspNodeCount")))


# Add a raw PC with name "MainNode" to the request.The MainNode contains the dsp_frontend and dsp_backend along with the database
#main_node = request.RawPC("MainNode")
#main_node.hardware_type = params.dspHardwareType
# Create lan
lan = request.LAN("lan")

# Create interface for main node
#iface_main = main_node.addInterface("if1")

# Add interface to lan
#lan.addInterface(iface_main)

# Install and execute a script that is contained in the repository on the main node
#main_node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/install_docker.sh"))
#main_node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/configure_docker_cred.sh"))
#main_node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/start_docker.sh"))
#main_node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/create_registry.sh"))
# Install Gitlab runner for running the gitlab ci jobs
#main_node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/install_gitlab_runner.sh"))




# Process worker nodes . Create interface and add to lan 
for i in range(params.dspNodeCount):
    name = "node" + str(i)
    node = request.RawPC(name)
    node.hardware_type = params.dspHardwareType
    iface = node.addInterface("if1")
    lan.addInterface(iface)
    node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/pre_conditioning.sh"))
    node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/download_datasets.sh"))
    node.addService(pg.Execute(shell="sh", command="sudo sh /local/repository/install_all_tools.sh"))
    
    
    

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
