# Activate RaFTS Processing No NHD Tests, then the Post Processing Python file 

# Instructions:
# 1. Modify input files from corresponding folders "xssa_nonhd_01" through "xssa_nonhd_08"
# 2. Activate by double clicking in Linux or follow steps below:
# 3. In terminal make the script executable using
# "chmod +x _Activate_RaFTS_and_PostProcessor.sh"
# 4. Run by calling 
# "./_Activate_RaFTS_and_PostProcessor.sh"


# Define the list of directories for analysis
directories=(
    "xssa_nonhd_01"
    "xssa_nonhd_02"
    "xssa_nonhd_03"
    "xssa_nonhd_04"
    "xssa_nonhd_05"
    "xssa_nonhd_06"
    "xssa_nonhd_07"
    "xssa_nonhd_08"
)

# Array to store collected run times
declare -a runtime_summary

echo "Starting batch processing of RaFTS experiments..."

for dir in "${directories[@]}"; do
    if [ -d "$dir" ]; then
        echo "Processing directory: $dir"
        
        # Enter directory
        cd "$dir" || exit 1
        
        # Ensure script is executable (safety check)
        chmod +x xssa_nonhd_proc_rafts_all.sh
        
        # Capture start time (seconds since epoch)
        start_time=$(date +%s)
        
        # Run the script
        ./xssa_nonhd_proc_rafts_all.sh
        
        # Capture end time
        end_time=$(date +%s)
        
        # Calculate duration
        duration=$(( end_time - start_time ))
        
        # Store result
        runtime_summary+=("$dir: ${duration} seconds")
        
        # Return to parent directory
        cd ..
    else
        echo "[WARNING] Directory $dir does not exist. Skipping."
        runtime_summary+=("$dir: Directory missing")
    fi
done

echo "Running final analysis..."

# Run the collection/analysis script
python3 xssa_nonhd_collect_algo_eval.py

echo ""
echo "       Script Execution Time Summary"
for line in "${runtime_summary[@]}"; do
    echo "$line"
done

echo "Done."
