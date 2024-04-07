use std::time::Duration;

use crate::{
    config::{Driver, Fleet},
    handler::{self, driver::DriverHandler, fleet::FleetHandler},
    service::{
        self,
        driver::DriverService,
        fleet::FleetService,
        pb::{driver_server::DriverServer, fleet_server::FleetServer},
    },
    state::State,
};

pub fn new_fleet_server(config: &Fleet, state: Box<dyn State>) -> FleetServer<FleetService> {
    let handler = FleetHandler::new(state, handler::fleet::Config {});
    let service = FleetService::new(
        handler,
        service::fleet::Config {
            message_expires_after: Duration::from_secs(config.message_expires_after),
        },
    );
    FleetServer::new(service)
        .max_decoding_message_size(config.max_decoding_message_size)
        .max_encoding_message_size(config.max_encoding_message_size)
}

pub fn new_driver_server(config: &Driver, state: Box<dyn State>) -> DriverServer<DriverService> {
    let handler = DriverHandler::new(state, handler::driver::Config {});
    let service = DriverService::new(
        handler,
        service::driver::Config {
            message_expires_after: Duration::from_secs(config.message_expires_after),
        },
    );
    DriverServer::new(service)
        .max_decoding_message_size(config.max_decoding_message_size)
        .max_encoding_message_size(config.max_encoding_message_size)
}
