function run(t)
    local result = {}
    result[#result + 1] = {
        ["host"] = "TestHost",
        ["service"] = "Test Service",
        ["level"] = "OK",
        ["description"] = "Test description",
    }
    return result
end
