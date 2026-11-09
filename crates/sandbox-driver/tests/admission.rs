use sandbox_driver::contract::{
    Descriptor, Error, ExecSpec, OneShotImage, OneShotSpec, SandboxSpec,
};
use serde_json::json;

fn descriptor() -> Descriptor {
    serde_json::from_value(json!({
        "schemas": {"kind":"test", "version":"1", "config_schema":{}},
        "namespace":{"id":"test:one", "scope":"process"},
        "isolation":"none", "durability":"process", "access":"owner_process",
        "sources":["image"], "locality":{}, "resources":[],
        "network":[], "ports":{"exposable":"none"}, "capabilities":{}
    }))
    .expect("descriptor")
}

#[test]
fn unsupported_requests_are_refused_even_when_false_or_zero() {
    let descriptor = descriptor();
    for (field, request) in [
        ("container.init", json!({"container":{"init":false}})),
        (
            "container.privileged",
            json!({"container":{"privileged":false}}),
        ),
        (
            "container.capabilities_add",
            json!({"container":{"capabilities_add":["NET_ADMIN"]}}),
        ),
        (
            "container.capabilities_drop",
            json!({"container":{"capabilities_drop":["ALL"]}}),
        ),
        ("container.dns", json!({"container":{"dns":["1.1.1.1"]}})),
        (
            "container.extra_hosts",
            json!({"container":{"extra_hosts":{"db":"127.0.0.1"}}}),
        ),
        (
            "container.shm_size_mb",
            json!({"container":{"shm_size_mb":64}}),
        ),
        ("timers.auto_stop_ms", json!({"timers":{"auto_stop_ms":0}})),
        (
            "timers.auto_archive_ms",
            json!({"timers":{"auto_archive_ms":0}}),
        ),
        (
            "timers.auto_delete_ms",
            json!({"timers":{"auto_delete_ms":0}}),
        ),
        ("timers.ttl_ms", json!({"timers":{"ttl_ms":0}})),
        ("resources.cpu_cores", json!({"resources":{"cpu_cores":1}})),
        ("resources.memory_mb", json!({"resources":{"memory_mb":1}})),
        ("resources.disk_mb", json!({"resources":{"disk_mb":1}})),
        ("resources.gpus", json!({"resources":{"gpus":1}})),
        ("network", json!({"network":{"mode":"block"}})),
        (
            "services",
            json!({"services":{"db":{"image":"postgres:16"}}}),
        ),
        ("source", json!({"source":"host_directory"})),
    ] {
        let mut value = json!({"source":{"image":"test"}});
        value
            .as_object_mut()
            .expect("object")
            .extend(request.as_object().expect("object").clone());
        let spec: SandboxSpec = serde_json::from_value(value).expect("spec");
        assert_eq!(
            spec.validate_for(&descriptor),
            Err(Error::Unsupported {
                capability: field.into(),
            })
        );
    }
}

#[test]
fn the_provider_default_network_needs_no_advertisement() {
    let mut descriptor = descriptor();
    descriptor.network = serde_json::from_value(json!(["allow_all"])).expect("network modes");
    for spec in [
        json!({"source":{"image":"test"}}),
        json!({"source":{"image":"test"}, "network":{"mode":"provider_default"}}),
        json!({"source":{"image":"test"}, "network":{"mode":"allow_all"}}),
    ] {
        let spec: SandboxSpec = serde_json::from_value(spec).expect("spec");
        spec.validate_for(&descriptor).expect("supported");
    }
}

#[test]
fn advertised_requests_pass_but_invalid_requests_do_not() {
    let mut descriptor = descriptor();
    descriptor.capabilities =
        serde_json::from_value(json!({"container_fields":["init"], "timers":["auto_stop"]}))
            .expect("caps");
    let mut spec: SandboxSpec = serde_json::from_value(
        json!({"source":{"image":"test"}, "container":{"init":false}, "timers":{"auto_stop_ms":0}}),
    )
    .expect("spec");
    spec.validate_for(&descriptor).expect("supported");
    spec.env.insert("BAD=KEY".into(), "secret".into());
    assert!(matches!(
        spec.validate_for(&descriptor),
        Err(Error::InvalidSpec { .. })
    ));
}

#[test]
fn exec_refuses_invalid_arguments_and_redacts_inputs() {
    for argv in [vec![], vec![""], vec!["echo", "nul\0arg"]] {
        assert!(ExecSpec::argv(argv).validate().is_err());
    }
    let mut spec = ExecSpec::argv(["echo", "argument-secret"]);
    spec.env.insert("TOKEN".into(), "env-secret".into());
    spec.validate().expect("valid argv");
    let debug = format!("{spec:?}");
    assert!(!debug.contains("argument-secret"));
    assert!(!debug.contains("env-secret"));
    spec.env.insert("TOKEN".into(), "nul\0value".into());
    assert!(spec.validate().is_err());
}

#[test]
fn one_shot_builds_from_a_workspace_context_and_keeps_the_image_entrypoint() {
    let mut spec: OneShotSpec = serde_json::from_value(json!({
        "image": {"build": {"context": "_actions/owner/repo", "tag": "owner-repo", "reuse": true}},
        "args": ["--flag", "argument-secret"],
        "env": {"TOKEN": "env-secret"}
    }))
    .expect("action build");
    spec.validate().expect("valid build");
    assert_eq!(spec.entrypoint, None);
    let debug = format!("{spec:?}");
    assert!(!debug.contains("argument-secret"));
    assert!(!debug.contains("env-secret"));
    spec.args.push("nul\0arg".into());
    assert!(spec.validate().is_err());

    for image in [
        OneShotImage::Image(String::new()),
        OneShotImage::Build {
            context:    String::new(),
            dockerfile: None,
            tag:        "tag".into(),
            reuse:      false,
        },
        OneShotImage::Build {
            context:    "context".into(),
            dockerfile: None,
            tag:        String::new(),
            reuse:      false,
        },
    ] {
        assert!(OneShotSpec::new(image).validate().is_err());
    }
}

#[test]
fn one_shot_refuses_workload_sources_and_unknown_build_fields() {
    for image in [
        json!({"snapshot": "snap-1"}),
        json!("host_directory"),
        json!({"dockerfile": "FROM alpine"}),
        json!({"build": {"context": ".", "tag": "t", "reuse": false, "cache": true}}),
    ] {
        assert!(serde_json::from_value::<OneShotSpec>(json!({"image": image})).is_err());
    }
}
