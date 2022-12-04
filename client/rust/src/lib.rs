mod client;
pub use client::*;

pub mod armada {

    pub mod api {
        include!("gen/api.rs");
    }
    pub mod google {
        pub mod api {
            include!("gen/google.api.rs");
        }
    }
    pub mod k8s {
        pub mod io {
            pub mod api {
                pub mod core {
                    pub mod v1 {
                        include!("gen/k8s.io.api.core.v1.rs");
                    }
                }
                pub mod networking {
                    pub mod v1 {
                        include!("gen/k8s.io.api.networking.v1.rs");
                    }
                }
            }

            pub mod apimachinery {
                pub mod pkg {
                    pub mod util {
                        pub mod intstr {
                            include!("gen/k8s.io.apimachinery.pkg.util.intstr.rs");
                        }
                    }
                    pub mod api {
                        pub mod resource {
                            include!("gen/k8s.io.apimachinery.pkg.api.resource.rs");
                        }
                    }

                    pub mod apis {
                        pub mod meta {
                            pub mod v1 {
                                include!("gen/k8s.io.apimachinery.pkg.apis.meta.v1.rs");
                            }
                        }
                    }

                    pub mod runtime {
                        include!("gen/k8s.io.apimachinery.pkg.runtime.rs");
                    }
                }
            }
        }
    }
}
