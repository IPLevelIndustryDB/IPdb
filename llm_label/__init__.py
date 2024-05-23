def llm_label(time_stamp, modes, model_path=""):
    if "gpt" in modes:
        from llm_label.gpt_label import gpt_label
        gpt_label(time_stamp)
    if "gemini" in modes:
        from llm_label.bard_label import bard_label
        bard_label(time_stamp)
    if "llama" in modes:
        from llm_label.llama_label import llama_label
        llama_label(time_stamp)
    if "glm" in modes:
        from llm_label.glm_label import glm_label
        glm_label(time_stamp)
    if "mistral" in modes:
        from llm_label.mistral_label import mistral_label
        mistral_label(time_stamp)
    if "combine" in modes:
        from llm_label.combine_label import combine_label
        if model_path == "":
            print("Please provide model_path for combine_label")
            return
        combine_label(time_stamp, model_path)
