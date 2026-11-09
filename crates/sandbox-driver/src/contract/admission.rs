use super::{ContainerSpec, Descriptor, Environment, Error, SandboxSpec};

impl Environment {
    /// Portable and descriptor checks for editing a template. Schema validation
    /// and credential resolution remain required at admission.
    pub fn validate(&self, descriptor: &Descriptor) -> Result<(), Error> {
        self.spec.validate_for(descriptor)
    }
}

impl SandboxSpec {
    /// Rejects unsupported explicit requests before mutation, including false
    /// container flags and disabled timers. Opaque provider options require
    /// schema/backend validation in addition to this check.
    pub fn validate_for(&self, descriptor: &Descriptor) -> Result<(), Error> {
        self.validate()?;
        let capabilities = &descriptor.capabilities;
        require(descriptor.sources.contains(&self.source.kind()), "source")?;
        for (capability, field) in self.resources.requested() {
            require(descriptor.resources.contains(&capability), field)?;
        }
        if let Some((capability, field)) = self.network.requested() {
            require(descriptor.network.contains(&capability), field)?;
        }
        for (capability, field) in self.timers.requested() {
            require(capabilities.timers.contains(&capability), field)?;
        }
        require(
            self.services.is_empty() || capabilities.services,
            "services",
        )?;
        for (capability, field) in self.container.iter().flat_map(ContainerSpec::requested) {
            require(capabilities.container_fields.contains(&capability), field)?;
        }
        Ok(())
    }
}

fn require(supported: bool, field: &str) -> Result<(), Error> {
    if supported {
        Ok(())
    } else {
        Err(Error::Unsupported {
            capability: field.to_owned(),
        })
    }
}
