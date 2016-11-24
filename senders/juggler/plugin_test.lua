function printtable()
    pprint("", table)
end

function pprint(key, t)
    for k, v in pairs(t) do
        if key ~= "" then
            k = key .. "." .. k
        end
        if type(v) == "table" then
            pprint(k, v)
        else
            print(k .. " is " .. string.format("%.3f", v))
        end
    end
end
