using Random
using StatsBase
using Optimization
using OptimizationOptimJL
using OptimizationBBO
using ForwardDiff
using Plots

# Julia script to simulate failure estimation and optimise parameters.

function neg_log_likelihood(As, Bs, Is, cs)
    llsum = 0
    for k = 1:length(cs)
        i = Is[k, 1]
        j = Is[k, 2]
        if cs[k]
            llsum += log(As[i]*Bs[j])
        else
            llsum += log(1 - As[i]*Bs[j])
        end
    end
    return -llsum
end

function neg_log_likelihood_gradient!(dAs, dBs, As, Bs, Is, cs)
    dAs .= 0
    dBs .= 0
    for k = 1:length(cs)
        i = Is[k, 1]
        j = Is[k, 2]
        dA, dB = neg_log_likelihood_gradient_inner(As[i], Bs[j], cs[k])
        dAs[i] += dA
        dBs[j] += dB
    end
    return dAs, dBs
end

function neg_log_likelihood_gradient_inner(A, B, c)
    if c
        dA = -1 / A
        dB = -1 / B
        return dA, dB
    else
        dA = B / (1 - A*B)
        dB = A / (1 - A*B)
        return dA, dB
    end
end

function f(u, p)
    Is, cs, n = p
    As = view(u, 1:n)
    Bs = view(u, n+1:length(u))
    return neg_log_likelihood(As, Bs, Is, cs)
end

function g(G, u, p)
    Is, cs, n = p
    dAs = view(G, 1:n)
    dBs = view(G, n+1:length(u))
    neg_log_likelihood_gradient!(dAs, dBs, As, Bs, Is, cs)
    return G
end

function global_optimization_solution(n, k, Is, cs)
    prob = OptimizationProblem(
        OptimizationFunction(f, grad=g),
        ones(n+k)./2,
        (Is, cs, n),
        lb=zeros(n+k),
        ub=ones(n+k),
    )
    return solve(prob, NelderMead())
end

function gd(Is, Js, cs, ts, n, k; inner_opt, outer_opt, update_interval::Float64=60.0, num_sub_iterations::Integer=1)
    nsamples = length(cs)
    x = fill(0.5, n+k)
    xk = zeros(n+k)
    g = zeros(n+k)
    As = zeros(n, nsamples)
    Bs = zeros(k, nsamples)
    Ak = view(x, 1:n)
    Bk = view(x, (n+1):(n+k))
    Aki = view(xk, 1:n)
    Bki = view(xk, (n+1):(n+k))
    Agki = view(g, 1:n)
    Bgki = view(g, (n+1):(n+k))

    index_of_last_update = 0
    time_of_last_update = 0.0
    for sample_index = 1:nsamples
        As[:, sample_index] .= Ak
        Bs[:, sample_index] .= Bk

        if !(ts[sample_index] - time_of_last_update >= update_interval)
            # Not yet time to update.
            continue
        end

        # Perform several gradient descent steps over the collected data.
        xk .= x
        batch_indices = collect((index_of_last_update+1):sample_index)
        for _ = 1:num_sub_iterations
            g .= 0
            shuffle!(batch_indices)
            for batch_index = batch_indices
                i = Is[batch_index]
                j = Js[batch_index]
                dA, dB = neg_log_likelihood_gradient_inner(Aki[i], Bki[j], cs[batch_index])
                Agki[i] += dA
                Bgki[j] += dB
            end

            Flux.Optimise.update!(inner_opt, xk, g)
            xk .= min.(max.(xk, 0), 1)
        end

        g .= x .- xk
        Flux.Optimise.update!(outer_opt, x, g)
        x .= min.(max.(x, 0), 1)

        index_of_last_update = sample_index
        time_of_last_update = ts[sample_index]

        As[:, sample_index] .= Ak
        Bs[:, sample_index] .= Bk
    end
    return As, Bs
end

struct NodeTemplate
    n::Int
    p::Float64
    df::Float64
end

struct QueueTemplate
    k::Int
    p::Float64
    df::Float64
    ds::Float64
    ws::Vector{Int}
end

struct Parameters
    # Minimum simulation time.
    t::Float64
    node_templates::Vector{NodeTemplate}
    queue_templates::Vector{QueueTemplate}
end

function scenario1()
    return Parameters(
        36000,
        [
            NodeTemplate(1, 0.9, 60),
            NodeTemplate(1, 0.01, 60),
        ],
        [
            QueueTemplate(1, 0.9, 60, 60, [1]),
            QueueTemplate(1, 0.01, 60, 60, [1]),
        ],
    )
end

function scenario2()
    return Parameters(
        36000,
        [
            NodeTemplate(18, 0.9, 60),
            NodeTemplate(2, 0.1, 60),
        ],
        [
            QueueTemplate(1, 0.9, 60, 60, [1]),
            QueueTemplate(1, 0.1, 60, 60, [1]),
        ],
    )
end

function scenario3()
    return Parameters(
        36000,
        [
            NodeTemplate(90, 0.9, 60),
            NodeTemplate(10, 0.1, 60),
        ],
        [
            QueueTemplate(8, 0.9, 60, 600, ones(Int, 8)),
            QueueTemplate(2, 0.1, 60, 600, ones(Int, 2)),
        ],
    )
end

function simulate(p::Parameters)
    for tmpl in p.queue_templates
        if length(tmpl.ws) != tmpl.k
            throw(ArgumentError("ws must be of length k"))
        end
    end
    n = sum(tmpl.n for tmpl in p.node_templates)
    k = sum(tmpl.k for tmpl in p.queue_templates)
    ws = [sum(tmpl.ws) for tmpl in p.queue_templates]
    ncs = cumsum(tmpl.n for tmpl in p.node_templates)
    kcs = cumsum(tmpl.k for tmpl in p.queue_templates)

    Is = Vector{Int}()
    Js = Vector{Int}()
    as = Vector{Bool}()
    bs = Vector{Bool}()
    cs = Vector{Bool}()
    ts = Vector{Float64}()
    for (node_template_index, node_template) in enumerate(p.node_templates)
        for node_template_i = 1:node_template.n
            i = node_template_i
            if node_template_index > 1
                i += ncs[node_template_index-1]
            end

            t = 0.0
            while t < p.t
                queue_template_index = sample(1:length(p.queue_templates), Weights(ws))
                queue_template = p.queue_templates[queue_template_index]
                j = sample(1:queue_template.k, Weights(queue_template.ws))
                if queue_template_index > 1
                    j += kcs[queue_template_index-1]
                end

                # Sample node and queue failure.
                a = rand() < node_template.p
                b = rand() < queue_template.p
                c = a*b

                # Compute duration until job termination.
                d = 0.0
                if !a
                    # Node failure.
                    d = node_template.df
                elseif !b
                    # Queue failure.
                    d = queue_template.df
                else
                    # Job success.
                    d = queue_template.ds
                end

                t += d
                push!(Is, i)
                push!(Js, j)
                push!(as, a)
                push!(bs, b)
                push!(cs, c)
                push!(ts, t)
            end
        end
    end

    # Sort by time and return.
    p = sortperm(ts)
    return Is[p], Js[p], as[p], bs[p], cs[p], ts[p]
end

function rate_by_index(Is, as)
    as_by_i = Dict{Int, Vector{Bool}}()
    for k = 1:length(Is)
        i = Is[k]
        a = as[k]
        vs = get(as_by_i, i, Vector{Bool}())
        as_by_i[i] = push!(vs, a)
    end
    rate_by_index = Dict{Int, Float64}()
    for (i, vs) in as_by_i
        rate_by_index[i] = sum(vs)/length(vs)
    end
    return rate_by_index
end

function squared_error(p::Parameters, As, Bs)
    Ase = copy(As)
    Bse = copy(Bs)
    i = 1
    for node_template = p.node_templates
        for _ = 1:node_template.n
            Ase[i, :] .-= node_template.p
            Ase[i, :] .^= 2
            i += 1
        end
    end
    j = 1
    for queue_template = p.queue_templates
        for _ = 1:queue_template.k
            Bse[j, :] .-= queue_template.p
            Bse[j, :] .^= 2
            j += 1
        end
    end
    return Ase, Bse
end

function grid_search(p::Parameters; num_simulations=10)
    n = sum(tmpl.n for tmpl in p.node_templates)
    k = sum(tmpl.k for tmpl in p.queue_templates)

    it = Base.Iterators.product(
        # Inner step-size.
        range(1e-5, 1e-2, length=10),
        # Number of sub-iterations.
        range(1, 10),
        # Outer step-size.
        range(1e-2, 0.25, length=10),
        # Outer Nesterov acceleration.
        range(0.0, 0.99, length=10)
    )
    mses = zeros(length(it))
    for _ = num_simulations
        Is, Js, as, bs, cs, ts = simulate(p)
        for (i, (inner_step_size, num_sub_iterations, outer_step_size, outer_nesterov_acceleration)) = enumerate(it)
            As, Bs = gd(Is, Js, cs, ts, n, k, inner_opt=Descent(inner_step_size), outer_opt=Nesterov(outer_step_size, outer_nesterov_acceleration), update_interval=60.0, num_sub_iterations=num_sub_iterations)
            Ase, Bse = squared_error(p, As, Bs)
            mses[i] += (sum(Ase) + sum(Bse)) / ((n+k)*length(cs))
        end
    end
    mses ./= num_simulations
    return it, mses
end

function main()
    p = scenario3()
    n = sum(tmpl.n for tmpl in p.node_templates)
    k = sum(tmpl.k for tmpl in p.queue_templates)

    Is, Js, as, bs, cs, ts = simulate(p)

    plots = Vector{Plots.Plot}()

    As, Bs = gd(Is, Js, cs, ts, n, k, inner_opt=Descent(0.05), outer_opt=Nesterov(0.05, 0.2), update_interval=60.0, num_sub_iterations=10)
    Ase, Bse = squared_error(p, As, Bs)
    push!(plots, plot!(plot(ts, As', legend=false), ts, mean(Ase, dims=1)', color="black", legend=false))
    push!(plots, plot!(plot(ts, Bs', legend=false), ts, mean(Bse, dims=1)', color="black", legend=false))

    As, Bs = gd(Is, Js, cs, ts, n, k, inner_opt=Descent(0.05), outer_opt=Nesterov(0.01, 0.9), update_interval=60.0, num_sub_iterations=10)
    Ase, Bse = squared_error(p, As, Bs)
    push!(plots, plot!(plot(ts, As', legend=false), ts, mean(Ase, dims=1)', color="black", legend=false))
    push!(plots, plot!(plot(ts, Bs', legend=false), ts, mean(Bse, dims=1)', color="black", legend=false))

    As, Bs = gd(Is, Js, cs, ts, n, k, inner_opt=Descent(0.05), outer_opt=Nesterov(0.005, 0.99), update_interval=60.0, num_sub_iterations=10)
    Ase, Bse = squared_error(p, As, Bs)
    push!(plots, plot!(plot(ts, As', legend=false), ts, mean(Ase, dims=1)', color="black", legend=false))
    push!(plots, plot!(plot(ts, Bs', legend=false), ts, mean(Bse, dims=1)', color="black", legend=false))

    plot(plots..., layout=(3, 2))
end
